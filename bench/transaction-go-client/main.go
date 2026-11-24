package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatal("usage: transaction-go-client bootstrap-servers topic")
	}
	brokers := kgo.SeedBrokers(strings.Split(os.Args[1], ",")...)
	topic := os.Args[2]
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	writer, err := kgo.NewClient(brokers, kgo.TransactionalID(topic+"-writer"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.ProducerBatchCompression(kgo.ZstdCompression()))
	check(err)
	defer writer.Close()

	request := kmsg.NewPtrCreateTopicsRequest()
	spec := kmsg.NewCreateTopicsRequestTopic()
	spec.Topic, spec.NumPartitions, spec.ReplicationFactor = topic, 2, 3
	request.Topics = append(request.Topics, spec)
	created, err := request.RequestWith(ctx, writer)
	check(err)
	if len(created.Topics) != 1 || created.Topics[0].ErrorCode != 0 {
		log.Fatalf("failed to create %s: %v", topic, created.Topics)
	}

	for _, transaction := range []struct {
		value  string
		commit kgo.TransactionEndTry
	}{{"committed-first", kgo.TryCommit}, {"aborted", kgo.TryAbort}, {"committed-last", kgo.TryCommit}} {
		check(writer.BeginTransaction())
		check(writer.ProduceSync(ctx,
			&kgo.Record{Topic: topic, Partition: 0, Value: []byte(transaction.value)},
			&kgo.Record{Topic: topic, Partition: 1, Value: []byte(transaction.value)}).FirstErr())
		check(writer.EndTransaction(ctx, transaction.commit))
	}
	verify(ctx, brokers, topic, kgo.ReadCommitted(), []string{"committed-first", "committed-last"}, []int64{0, 4})
	verify(ctx, brokers, topic, kgo.ReadUncommitted(), []string{"committed-first", "aborted", "committed-last"}, []int64{0, 2, 4})
	fmt.Printf("franz-go verified commit, abort and both isolation levels for %s\n", topic)
}

func verify(ctx context.Context, brokers kgo.Opt, topic string, isolation kgo.IsolationLevel, values []string, offsets []int64) {
	consumer, err := kgo.NewClient(brokers, kgo.FetchIsolationLevel(isolation),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart(), 1: kgo.NewOffset().AtStart()}}))
	check(err)
	defer consumer.Close()
	var received [2][]string
	var positions [2][]int64
	for len(received[0]) < len(values) || len(received[1]) < len(values) {
		fetches := consumer.PollFetches(ctx)
		check(fetches.Err())
		fetches.EachRecord(func(record *kgo.Record) {
			received[record.Partition] = append(received[record.Partition], string(record.Value))
			positions[record.Partition] = append(positions[record.Partition], record.Offset)
		})
	}
	for partition := range received {
		if !reflect.DeepEqual(received[partition], values) || !reflect.DeepEqual(positions[partition], offsets) {
			log.Fatalf("partition %d returned %v at offsets %v", partition, received[partition], positions[partition])
		}
	}
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
