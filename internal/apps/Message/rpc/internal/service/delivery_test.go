package service

import "testing"

// MaybeDuplicate 是「跳过判重查询」这一优化的全部判据，
// 它的每个分支都对应一类真实的投递场景，逐一钉死。
func TestDeliveryMaybeDuplicate(t *testing.T) {
	cases := []struct {
		name             string
		numDelivered     uint64
		predatesConsumer bool
		want             bool
		scenario         string
	}{
		{
			name:         "first delivery",
			numDelivered: 1,
			want:         false,
			scenario:     "首投：消费侧不可能重复，客户端重发被发布侧去重窗口拦在 stream 之外",
		},
		{
			name:         "redelivered",
			numDelivered: 2,
			want:         true,
			scenario:     "落库成功后 Nak / 崩溃未 Ack 导致的重投，必须查重",
		},
		{
			name:         "redelivered many times",
			numDelivered: 5,
			want:         true,
			scenario:     "接近 MaxDeliver 的多次重投同样要查",
		},
		{
			name:         "metadata missing",
			numDelivered: 0,
			want:         true,
			scenario:     "DLQ 重放等元数据缺失场景，无从判断，保守查重",
		},
		{
			name:             "first delivery after consumer rebuilt",
			numDelivered:     1,
			predatesConsumer: true,
			want:             true,
			scenario:         "consumer 删除重建后存量消息从头重投、计数归零，可能已被前一个 consumer 落过库",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := Delivery{NumDelivered: c.numDelivered, PredatesConsumer: c.predatesConsumer}
			if got := d.MaybeDuplicate(); got != c.want {
				t.Fatalf("NumDelivered=%d PredatesConsumer=%v: got %v want %v —— %s",
					c.numDelivered, c.predatesConsumer, got, c.want, c.scenario)
			}
		})
	}
}

// 零值 Delivery（元数据完全缺失）必须同时落到两条保守路径上：
// seq 分配退化为本地逻辑时钟，且判重查询照常执行。
func TestZeroDeliveryIsConservative(t *testing.T) {
	var d Delivery
	if d.StreamSeq != 0 {
		t.Fatalf("zero Delivery must carry StreamSeq 0, got %d", d.StreamSeq)
	}
	if !d.MaybeDuplicate() {
		t.Fatal("zero Delivery must be treated as possibly duplicate")
	}
}
