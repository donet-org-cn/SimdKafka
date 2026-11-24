namespace SimdKafka;

internal static class ProducerSequence
{
    internal static int Next(RecordBatch batch) => (int)(((long)batch.Sequence + batch.Count) & int.MaxValue);

    internal static Error Validate(RecordBatch batch, short currentEpoch, int next, bool logEmpty)
    {
        if (batch.Epoch < currentEpoch) return Error.InvalidProducerEpoch;
        if (currentEpoch < 0) return !logEmpty || batch.Sequence == 0 ? Error.None : Error.OutOfOrderSequence;
        return batch.Sequence == (batch.Epoch == currentEpoch ? next : 0) ? Error.None : Error.OutOfOrderSequence;
    }
}
