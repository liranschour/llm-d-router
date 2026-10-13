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

	. "github.com/onsi/ginkgo/v2" // nolint:revive
	. "github.com/onsi/gomega"    // nolint:revive

	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"
	"github.com/llm-d/llm-d-router/pkg/sidecar/constants"
)

// fetchPayload digs the kv.fetch payload out of an envelope, asserting the
// envelope shape on the way down.
func fetchPayload(hints map[string]any) map[string]any {
	GinkgoHelper()

	Expect(hints).To(HaveKeyWithValue(hintFieldProtocolVersion, kvHintProtocolVersion))
	Expect(hints[hintFieldMessageID]).ToNot(BeEmpty())

	actions, ok := hints[hintFieldActions].([]any)
	Expect(ok).To(BeTrue(), "actions must be a list")
	Expect(actions).To(HaveLen(1))

	action, ok := actions[0].(map[string]any)
	Expect(ok).To(BeTrue(), "action must be a mapping")
	Expect(action).To(HaveKeyWithValue(hintFieldActionType, kvFetchActionType))
	Expect(action).To(HaveKeyWithValue(hintFieldActionVersion, kvFetchActionVersion))
	Expect(action[hintFieldActionID]).ToNot(BeEmpty())

	payload, ok := action[hintFieldPayload].(map[string]any)
	Expect(ok).To(BeTrue(), "payload must be a mapping")
	return payload
}

var _ = DescribeTable("kvcrPullAvailable",
	func(connector string, enableKVCRPull, want bool) {
		s := &Server{config: Config{KVConnector: connector, EnableKVCRPull: enableKVCRPull}}
		Expect(s.kvcrPullAvailable()).To(Equal(want))
	},
	Entry("kvcr is always available", constants.KVConnectorKVCR, false, true),
	Entry("nixlv2 with the flag is available", constants.KVConnectorNIXLV2, true, true),
	Entry("nixlv2 without the flag is unavailable", constants.KVConnectorNIXLV2, false, false),
	Entry("offloading is the P2P tier, not KVCR", constants.KVConnectorOffloading, false, false),
	Entry("the flag has no effect on sglang", constants.KVConnectorSGLang, true, false),
)

var _ = DescribeTable("kvcrControlEndpoint",
	func(dpSize, dpBasePort int, target, want string) {
		s := &Server{
			dpBasePort: dpBasePort,
			config:     Config{KVCRControlPort: 17771, DataParallelSize: dpSize},
		}
		Expect(s.kvcrControlEndpoint(target)).To(Equal(want))
	},
	Entry("single DP uses the base port regardless of target", 1, 8000, "10.0.0.5:8003", "tcp://10.0.0.5:17771"),
	Entry("rank 0 target uses the base port", 4, 8000, "10.0.0.5:8000", "tcp://10.0.0.5:17771"),
	Entry("rank 2 target offsets by 2", 4, 8000, "10.0.0.5:8002", "tcp://10.0.0.5:17773"),
	Entry("last rank target offsets by dpSize-1", 4, 8000, "10.0.0.5:8003", "tcp://10.0.0.5:17774"),
	Entry("port below the base falls back to rank 0", 4, 8000, "10.0.0.5:7999", "tcp://10.0.0.5:17771"),
	Entry("port beyond the rank range falls back to rank 0", 4, 8000, "10.0.0.5:8004", "tcp://10.0.0.5:17771"),
	Entry("target without a port falls back to rank 0", 4, 8000, "10.0.0.5", "tcp://10.0.0.5:17771"),
	Entry("unparsable port falls back to rank 0", 4, 8000, "10.0.0.5:http", "tcp://10.0.0.5:17771"),
	Entry("zero base port disables derivation", 4, 0, "10.0.0.5:8002", "tcp://10.0.0.5:17771"),
	Entry("scheme-prefixed target derives the rank", 4, 8000, "http://10.0.0.5:8002", "tcp://10.0.0.5:17773"),
	// JoinHostPort must bracket the literal, or the engine cannot parse the
	// endpoint back into host and port.
	Entry("IPv6 literal stays bracketed", 1, 0, "[fd00::5]:8000", "tcp://[fd00::5]:17771"),
	Entry("IPv6 literal derives the rank too", 4, 8000, "[fd00::5]:8002", "tcp://[fd00::5]:17773"),
)

var _ = Describe("kvcrFetchHints", func() {
	newServer := func() *Server {
		return &Server{
			dpBasePort: 8000,
			config:     Config{KVCRControlPort: 17771, DataParallelSize: 4},
		}
	}

	It("names the source's control endpoint, offset by its DP rank", func() {
		payload := fetchPayload(newServer().kvcrFetchHints("10.0.0.9:8002"))
		Expect(payload).To(HaveKeyWithValue(hintFieldSourceControlEndpoint, "tcp://10.0.0.9:17773"))
	})

	It("requests copy mode and omits no_retain, which the parser rejects", func() {
		payload := fetchPayload(newServer().kvcrFetchHints("10.0.0.9:8002"))
		Expect(payload).To(HaveKeyWithValue(hintFieldMode, kvFetchModeCopy))
		Expect(payload).ToNot(HaveKey("no_retain"))
	})

	It("sends a non-empty block_hashes list, which the parser requires", func() {
		payload := fetchPayload(newServer().kvcrFetchHints("10.0.0.9:8002"))
		hashes, ok := payload[hintFieldBlockHashes].([]uint64)
		Expect(ok).To(BeTrue(), "block_hashes must be a uint64 list")
		Expect(hashes).To(Equal([]uint64{placeholderBlockHash}))
	})

	// The parser rejects bools and non-integers outright, so a marshalling slip
	// here would silently disable every hint rather than fail loudly.
	It("marshals the placeholder as a JSON integer, not a float, string or bool", func() {
		body, err := json.Marshal(newServer().kvcrFetchHints("10.0.0.9:8002"))
		Expect(err).ToNot(HaveOccurred())
		Expect(string(body)).To(ContainSubstring(`"block_hashes":[1]`))
	})

	It("gives each envelope a fresh message_id and a matching action_id", func() {
		s := newServer()
		first := s.kvcrFetchHints("10.0.0.9:8002")
		second := s.kvcrFetchHints("10.0.0.9:8002")
		Expect(first[hintFieldMessageID]).ToNot(Equal(second[hintFieldMessageID]))

		action := first[hintFieldActions].([]any)[0].(map[string]any)
		Expect(action[hintFieldActionID]).To(Equal(first[hintFieldMessageID].(string) + "-fetch"))
	})

	It("keeps rank derivation on Clone, which rank servers rely on", func() {
		payload := fetchPayload(newServer().Clone().kvcrFetchHints("10.0.0.9:8002"))
		Expect(payload).To(HaveKeyWithValue(hintFieldSourceControlEndpoint, "tcp://10.0.0.9:17773"))
	})
})

var _ = Describe("addKVCRPullToPrefill", func() {
	newServer := func() *Server {
		return &Server{
			dpBasePort: 8000,
			config:     Config{KVCRControlPort: 17771, DataParallelSize: 8},
		}
	}

	It("injects a pull when source and prefiller are different ranks on the same pod", func() {
		data := map[string]any{}

		newServer().addKVCRPullToPrefill(data, "10.0.6.107:8003", "10.0.6.107:8007")

		hints, ok := data[reqcommon.FieldKVHints].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(fetchPayload(hints)).To(
			HaveKeyWithValue(hintFieldSourceControlEndpoint, "tcp://10.0.6.107:17774"))
	})

	It("does not inject when the source is the prefiller itself", func() {
		data := map[string]any{}

		newServer().addKVCRPullToPrefill(data, "10.0.6.107:8003", "10.0.6.107:8003")

		Expect(data).ToNot(HaveKey(reqcommon.FieldKVHints))
	})

	It("treats a scheme-prefixed source as the same endpoint", func() {
		data := map[string]any{}

		newServer().addKVCRPullToPrefill(data, "http://10.0.6.107:8003", "10.0.6.107:8003")

		Expect(data).ToNot(HaveKey(reqcommon.FieldKVHints))
	})

	It("does not inject when no source is named", func() {
		data := map[string]any{}

		newServer().addKVCRPullToPrefill(data, "", "10.0.6.107:8003")

		Expect(data).ToNot(HaveKey(reqcommon.FieldKVHints))
	})
})

var _ = Describe("addCachedPrefixPullToPrefill", func() {
	newServer := func(connector string, enableKVCRPull bool) *Server {
		return &Server{
			dpBasePort: 8000,
			config: Config{
				KVConnector:      connector,
				EnableKVCRPull:   enableKVCRPull,
				KVCRControlPort:  17771,
				P2PConnectorPort: 7777,
				DataParallelSize: 4,
			},
		}
	}

	// A NIXLv2 PD deployment composing a pull must emit the field its own
	// secondary tier reads, not whichever injector the caller happened to call.
	It("emits kv_hints when a KVCR pull is declared alongside NIXLv2", func() {
		data := map[string]any{reqcommon.FieldKVTransferParams: map[string]any{}}

		newServer(constants.KVConnectorNIXLV2, true).
			addCachedPrefixPullToPrefill(data, "10.0.0.9:8002", "10.0.0.5:8000")

		Expect(fetchPayload(data[reqcommon.FieldKVHints].(map[string]any))).To(
			HaveKeyWithValue(hintFieldSourceControlEndpoint, "tcp://10.0.0.9:17773"))
		// The NIXL params must survive untouched: no remote_kv_source added.
		Expect(data[reqcommon.FieldKVTransferParams]).To(Equal(map[string]any{}))
	})

	It("emits remote_kv_source when a P2P pull is declared alongside NIXLv2", func() {
		data := map[string]any{reqcommon.FieldKVTransferParams: map[string]any{}}

		newServer(constants.KVConnectorNIXLV2, false).
			addCachedPrefixPullToPrefill(data, "10.0.0.9:8002", "10.0.0.5:8000")

		Expect(data).ToNot(HaveKey(reqcommon.FieldKVHints))
		kvParams := data[reqcommon.FieldKVTransferParams].(map[string]any)
		source, ok := kvParams[requestFieldRemoteKVSource].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(source).To(HaveKeyWithValue(reqcommon.FieldRemotePort, 7779))
	})

	It("does not panic when there are no kv_transfer_params to nest under", func() {
		data := map[string]any{}

		Expect(func() {
			newServer(constants.KVConnectorNIXLV2, false).
				addCachedPrefixPullToPrefill(data, "10.0.0.9:8002", "10.0.0.5:8000")
		}).ToNot(Panic())
		Expect(data).To(BeEmpty())
	})
})
