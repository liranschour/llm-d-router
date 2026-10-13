/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package proxy

import (
	"encoding/json"
	"maps"
	"net"
	"net/http"
	"os"
	"strconv"

	"github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"
	"github.com/llm-d/llm-d-router/pkg/sidecar/constants"
)

// Router KV hint envelope fields, as vLLM's kv_hints parser reads them. The
// envelope is a sibling of kv_transfer_params on the request body, not a
// subfield of it.
const (
	hintFieldProtocolVersion = "protocol_version"
	hintFieldMessageID       = "message_id"
	hintFieldActions         = "actions"
	hintFieldActionID        = "action_id"
	hintFieldActionType      = "action_type"
	hintFieldActionVersion   = "action_version"
	hintFieldPayload         = "payload"

	hintFieldSourceControlEndpoint = "source_control_endpoint"
	hintFieldBlockHashes           = "block_hashes"
	hintFieldMode                  = "mode"
)

// Envelope and action versions KVCR's hint parser understands. A mismatch is
// only warned about engine-side, but keep them exact.
const (
	kvHintProtocolVersion = "0.1"
	kvFetchActionType     = "kv.fetch"
	kvFetchActionVersion  = "1.0"

	// kvFetchModeCopy leaves the source's copy in place. It is also the
	// parser's default, but is sent explicitly so the intent is on the wire.
	// Any other value is rejected outright.
	kvFetchModeCopy = "copy"
)

// placeholderBlockHash satisfies the parser's rule that block_hashes be a
// non-empty list of uint64-range integers. KVCR never compares the value:
// opportunistic_query, which the vLLM adapter enables by default, skips the
// hash-membership test, so the hint means "this request may pull from this
// peer" and the engine runs its own per-key lookup and load against it. That
// is the same request-scoped contract the P2P tier already has, which is why
// the sidecar never needs the real block hashes — it would have to reproduce
// the engine's hashing bit for bit, and a mismatch degrades to a silent miss.
//
// A deployment that sets opportunistic_query false is therefore unsupported
// here: its engine would compare these hashes and every hint would miss.
const placeholderBlockHash uint64 = 1

// handleKVCR implements the vLLM OffloadingConnector contract for a KVCR
// secondary tier. Unlike the P2P tier it needs nothing injected on the prefill
// request: a KVCR producer is passive, holding KV in vLLM's own host tier and
// answering a peer's start_write by pinning those chunks in place. Only the
// decode request carries metadata, a kv_hints envelope naming the prefiller's
// ZMQ control endpoint.
//
// The prefill request runs to completion before decode is dispatched. KVCR
// transfers are destination-initiated, so the decoder's fetch succeeds only
// once the prefiller already holds the blocks.
func (s *Server) handleKVCR(w http.ResponseWriter, r *http.Request, prefillPodHostPort, kvCacheSource string, apiType reqcommon.APIType) {
	_, requestData, ok := s.readJSONBody(r, w)
	if !ok {
		return
	}

	s.logger.Info("running KVCR protocol",
		"prefill_host", extractHost(prefillPodHostPort),
		"kvcr_control_endpoint", s.kvcrControlEndpoint(prefillPodHostPort))

	// Prefill request: no KV metadata at all, just capped to a single output
	// token so the prefiller returns as soon as the prompt is computed.
	prefillData := maps.Clone(requestData)
	s.addKVCRPullToPrefill(prefillData, kvCacheSource, prefillPodHostPort)
	reqcommon.CapSingleToken(prefillData, apiType)

	prefillBody, err := json.Marshal(prefillData)
	if err != nil {
		if err := errorJSONInvalid(err, w); err != nil {
			s.logger.Error(err, "failed to send error response to client")
		}
		return
	}
	if v := s.logger.V(logging.TRACE); v.Enabled() {
		v.Info("prefill request body", logging.HTTPBodyKey, string(prefillBody))
	}

	// Decode request: fetch the prefiller's KV over KVCR. Original body
	// (streaming, token limits) is preserved.
	decodeData := maps.Clone(requestData)
	decodeData[reqcommon.FieldKVHints] = s.kvcrFetchHints(prefillPodHostPort)

	decodeBody, err := json.Marshal(decodeData)
	if err != nil {
		if err := errorJSONInvalid(err, w); err != nil {
			s.logger.Error(err, "failed to send error response to client")
		}
		return
	}
	if v := s.logger.V(logging.TRACE); v.Enabled() {
		v.Info("decode request body", logging.HTTPBodyKey, string(decodeBody))
	}

	s.handleSequentialPD(w, r, prefillBody, decodeBody, prefillPodHostPort, constants.KVConnectorKVCR)
}

// kvcrPullAvailable reports whether this deployment can pull cached prefix over
// a KVCR secondary tier. That tier is the PD connector itself when KVConnector
// is kvcr, or is composed alongside NIXL via MultiConnector (declared with
// --enable-kvcr-pull) when the PD connector is NIXLv2. On any other connector
// --enable-kvcr-pull has no effect, since no MultiConnector routes a kv_hints
// envelope to an OffloadingConnector with a KVCR tier.
func (s *Server) kvcrPullAvailable() bool {
	return s.config.KVConnector == constants.KVConnectorKVCR ||
		(s.config.EnableKVCRPull && s.config.KVConnector == constants.KVConnectorNIXLV2)
}

// addKVCRPullToPrefill adds a kv_hints fetch to a prefill request so the
// prefiller pulls cached prefix from kvCacheSource while keeping its own
// computed blocks available for the decoder. It is a no-op when no source is
// set or the source is the selected prefill endpoint, since there is nothing to
// pull from oneself.
func (s *Server) addKVCRPullToPrefill(prefillData map[string]any, kvCacheSource, prefillPodHostPort string) {
	source := normalizeEndpoint(kvCacheSource)
	prefiller := normalizeEndpoint(prefillPodHostPort)
	if source != "" && source != prefiller {
		prefillData[reqcommon.FieldKVHints] = s.kvcrFetchHints(source)
	}
}

// addCachedPrefixPullToPrefill injects the prefill-side cached-prefix pull in
// whichever protocol this deployment's secondary tier speaks: a kv_hints
// envelope for KVCR, a kv_transfer_params.remote_kv_source block for the P2P
// tier. Callers that compose a pull alongside another PD connector (NIXLv2) go
// through here rather than calling either injector directly, so declaring
// --enable-kvcr-pull does not leave them emitting P2P params.
//
// prefillRequest is the whole request body, since kv_hints is a top-level
// field while remote_kv_source nests under kv_transfer_params.
func (s *Server) addCachedPrefixPullToPrefill(prefillRequest map[string]any, kvCacheSource, prefillPodHostPort string) {
	if s.kvcrPullAvailable() {
		s.addKVCRPullToPrefill(prefillRequest, kvCacheSource, prefillPodHostPort)
		return
	}
	kvParams, ok := prefillRequest[reqcommon.FieldKVTransferParams].(map[string]any)
	if !ok {
		// No kv_transfer_params to nest under: the caller has not built the
		// field, so there is nothing a P2P pull could attach to.
		return
	}
	s.addP2PPullToPrefill(kvParams, kvCacheSource, prefillPodHostPort)
}

// kvcrControlEndpoint returns the peer's KVCR ZMQ control endpoint in the form
// the kv.fetch payload names it. The host comes from the already-validated
// routing header; the port is derived locally from configuration, so this adds
// no SSRF surface beyond the host check the dispatcher already applied.
//
// KVCR indexes control_ports by dp_local_rank, the same pod-local rank the
// routed HTTP port encodes, so the base port is offset by rankFor.
func (s *Server) kvcrControlEndpoint(targetHostPort string) string {
	host := extractHost(targetHostPort)
	port := s.config.KVCRControlPort + s.rankFor(targetHostPort, "KVCR")
	// JoinHostPort brackets IPv6 literals, which a bare host:port concat would
	// leave ambiguous.
	return "tcp://" + net.JoinHostPort(host, strconv.Itoa(port))
}

// kvcrFetchHints builds the router kv_hints envelope that asks the engine to
// fetch this request's KV from the peer at sourceHostPort. block_hashes is the
// placeholder; see placeholderBlockHash for why the router names a peer rather
// than a block list.
func (s *Server) kvcrFetchHints(sourceHostPort string) map[string]any {
	messageID := newUUID()
	return map[string]any{
		hintFieldProtocolVersion: kvHintProtocolVersion,
		hintFieldMessageID:       messageID,
		hintFieldActions: []any{
			map[string]any{
				hintFieldActionID:      messageID + "-fetch",
				hintFieldActionType:    kvFetchActionType,
				hintFieldActionVersion: kvFetchActionVersion,
				hintFieldPayload: map[string]any{
					hintFieldSourceControlEndpoint: s.kvcrControlEndpoint(sourceHostPort),
					hintFieldBlockHashes:           []uint64{placeholderBlockHash},
					hintFieldMode:                  kvFetchModeCopy,
				},
			},
		},
	}
}

// decodeWithKVCRSource serves a decoder-only request through the local vLLM
// with a kv_hints fetch injected, so the engine looks up and pulls cached
// prefix blocks from the peer at sourceHostPort instead of recomputing them.
// It replaces any client-supplied kv_hints (the sidecar owns that field) and
// routes through dispatchDecode so chunked decode still applies.
//
// kv_transfer_params is deliberately left untouched: a NIXLv2 PD deployment
// composing a KVCR pull needs its NIXL params to reach the engine intact.
//
// When sourceHostPort resolves to this rank's own serving endpoint, injecting
// would tell the engine to pull the prefix it is already computing, so it
// decodes normally; another rank on the same pod is a valid peer and does get
// the injection.
func (s *Server) decodeWithKVCRSource(w http.ResponseWriter, r *http.Request, sourceHostPort string) {
	raw, requestData, ok := s.readJSONBody(r, w)
	if !ok {
		return
	}

	source := normalizeEndpoint(sourceHostPort)
	self := normalizeEndpoint(net.JoinHostPort(os.Getenv("POD_IP"), s.config.Port))
	if source == self {
		s.logger.V(logging.DEBUG).Info("KV cache source is this rank's endpoint, skipping kvcr injection",
			"source", sourceHostPort, "self", self)
		s.dispatchDecode(w, cloneRequestWithBody(r.Context(), r, raw), requestData)
		return
	}

	// Rebuild kv_hints from scratch: the sidecar owns this field, so
	// client-supplied values are dropped rather than forwarded to vLLM.
	requestData[reqcommon.FieldKVHints] = s.kvcrFetchHints(source)

	s.logger.Info("running KVCR source protocol",
		"source_host", extractHost(source),
		"kvcr_control_endpoint", s.kvcrControlEndpoint(source))

	newBody, err := json.Marshal(requestData)
	if err != nil {
		if err := errorJSONInvalid(err, w); err != nil {
			s.logger.Error(err, "failed to send error response to client")
		}
		return
	}
	if v := s.logger.V(logging.TRACE); v.Enabled() {
		v.Info("decoder request body with kvcr source", logging.HTTPBodyKey, string(newBody))
	}

	s.dispatchDecode(w, cloneRequestWithBody(r.Context(), r, newBody), requestData)
}
