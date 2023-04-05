package pstest

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	pb "cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestRetryCooldownAfterNack(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pclient, sclient, svr, cleanup := newFake(ctx, t)
	defer cleanup()

	advance, restore := mockServerClock(t, svr)
	defer restore()

	top := mustCreateTopic(ctx, t, pclient, &pb.Topic{Name: "projects/P/topics/T"})
	cooldownPeriod := 5 * time.Second
	sub := mustCreateSubscription(ctx, t, sclient, &pb.Subscription{
		Name:               "projects/P/subscriptions/S",
		Topic:              top.Name,
		AckDeadlineSeconds: 10,
		RetryPolicy:        &pb.RetryPolicy{MinimumBackoff: durationpb.New(cooldownPeriod)},
	})

	publish(t, pclient, top, []*pb.PubsubMessage{
		{Data: []byte("d1")},
		{Data: []byte("d2")},
		{Data: []byte("d3")},
	})

	got := map[string]int{}
	spc := mustStartStreamingPull(ctx, t, sclient, sub)

	rsps := make(chan *pb.StreamingPullResponse)
	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(rsps)
		for {
			rsp, err := spc.Recv()
			if err == io.EOF || status.Code(err) == codes.Canceled {
				return
			}
			if err != nil {
				t.Error(err)
				return
			}
			select {
			case rsps <- rsp:
			case <-ctx.Done():
				return
			}
		}
	}()
	// past this point, force cancel on the above loop before we wg.Wait()
	defer cancel()

	assertPullMessages(t, 3, rsps, func(rsp *pb.StreamingPullResponse) {
		var ackIDs []string
		for _, m := range rsp.ReceivedMessages {
			got[m.Message.MessageId]++
			ackIDs = append(ackIDs, m.AckId)
		}
		if _, err := sclient.ModifyAckDeadline(ctx, &pb.ModifyAckDeadlineRequest{
			Subscription:       sub.Name,
			AckIds:             ackIDs,
			AckDeadlineSeconds: 0,
		}); err != nil {
			t.Fatal(err)
		}
	})
	assertNoMessages(t, rsps)

	// Having nacked all three messages, we should see them again, but only after the retry cooldown period.
	advance(5 * time.Second)
	assertPullMessages(t, 3, rsps, func(rsp *pb.StreamingPullResponse) {
		for _, m := range rsp.ReceivedMessages {
			got[m.Message.MessageId]++
		}
	})

	for id, n := range got {
		if n != 2 {
			t.Errorf("message %s: saw %d times, want 2", id, n)
		}
	}
}

func TestRetryExponentialBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pclient, sclient, svr, cleanup := newFake(ctx, t)
	defer cleanup()

	advance, restore := mockServerClock(t, svr)
	defer restore()

	minAckDeadlineSecs = 1
	top := mustCreateTopic(ctx, t, pclient, &pb.Topic{Name: "projects/P/topics/T"})
	sub := mustCreateSubscription(ctx, t, sclient, &pb.Subscription{
		Name:               "projects/P/subscriptions/S",
		Topic:              top.Name,
		AckDeadlineSeconds: minAckDeadlineSecs,
		RetryPolicy: &pb.RetryPolicy{
			MinimumBackoff: durationpb.New(2 * time.Second),
			MaximumBackoff: durationpb.New(5 * time.Second),
		},
	})

	publish(t, pclient, top, []*pb.PubsubMessage{
		{Data: []byte("d1")},
		{Data: []byte("d2")},
		{Data: []byte("d3")},
	})

	got := map[string]int{}
	spc := mustStartStreamingPull(ctx, t, sclient, sub)

	rsps := make(chan *pb.StreamingPullResponse)
	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(rsps)
		for {
			rsp, err := spc.Recv()
			if err == io.EOF || status.Code(err) == codes.Canceled {
				return
			}
			if err != nil {
				t.Error(err)
				return
			}
			select {
			case rsps <- rsp:
			case <-ctx.Done():
				return
			}
		}
	}()
	// past this point, force cancel on the above loop before we wg.Wait()
	defer cancel()

	gotMsg := func(rsp *pb.StreamingPullResponse) {
		for _, m := range rsp.ReceivedMessages {
			got[m.Message.MessageId]++
		}
	}
	assertPullMessages(t, 3, rsps, gotMsg)
	assertNoMessages(t, rsps)

	// Advance 1 ack + 2 retry seconds
	advance(3 * time.Second)
	assertPullMessages(t, 3, rsps, gotMsg)
	assertNoMessages(t, rsps)

	// Advance 1 ack + 4 retry seconds
	advance(5 * time.Second)
	assertPullMessages(t, 3, rsps, gotMsg)
	assertNoMessages(t, rsps)

	// Advance 1 ack + 5 retry seconds
	advance(6 * time.Second)
	assertPullMessages(t, 3, rsps, gotMsg)
	assertNoMessages(t, rsps)

	// One more second won't help.
	advance(1 * time.Second)
	assertNoMessages(t, rsps)

	for id, n := range got {
		if n != 4 {
			t.Errorf("message %s: saw %d times, want 4", id, n)
		}
	}
}

func assertPullMessages(t *testing.T, n int, rsps <-chan *pb.StreamingPullResponse, h func(rsp *pb.StreamingPullResponse)) {
	t.Helper()
	seen := 0
	for seen < n {
		select {
		case rsp, ok := <-rsps:
			if !ok {
				t.Fatalf("unexpected channel close")
			}
			h(rsp)
			seen += len(rsp.ReceivedMessages)
		case <-time.After(2 * time.Second):
			t.Fatalf("failed to pull %d messages", n)
		}
	}
}

func assertNoMessages(t *testing.T, rsps <-chan *pb.StreamingPullResponse) {
	t.Helper()
	done := time.After(20 * time.Millisecond)
	for {
		select {
		case rsp, ok := <-rsps:
			if !ok {
				t.Fatalf("unexpected channel close")
			}
			if len(rsp.ReceivedMessages) > 0 {
				t.Fatalf("should not see messages")
			}
		case <-done: // 2x internal loop timer
			return // ok
		}
	}
}
