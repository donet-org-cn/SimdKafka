using System.Text.RegularExpressions;

namespace SimdKafka;

internal sealed partial class ClusterRouter
{
    private static readonly string[] TransactionStateNames =
        ["Empty", "Ongoing", "PrepareCommit", "PrepareAbort", "CompleteCommit", "CompleteAbort"];

    private async ValueTask<bool> DescribeTransactionsAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request, CancellationToken stop)
    {
        var r = new WireReader(body.Span);
        var names = new string[r.ArrayLength(true)];
        for (int i = 0; i < names.Length; i++) names[i] = r.Text(true);
        r.Tags();
        r.End();

        Error coordinator = await CoordinatorReadAsync(stop);
        var image = cluster.Metadata.Image;
        w.I32(0);
        w.ArrayLength(names.Length, true);
        foreach (string name in names)
        {
            bool allowed = request.AllowsTransaction(name, AclOperation.Describe);
            var state = allowed && coordinator == Error.None ? image.Transactions.GetValueOrDefault(name) : null;
            Error error = !allowed ? Error.TransactionalIdAuthorizationFailed : name.Length == 0 ? Error.InvalidRequest :
                coordinator != Error.None ? coordinator :
                state is null ? Error.TransactionalIdNotFound : Error.None;
            w.Write(error);
            w.Text(name, true);
            w.Text(state is null ? "" : TransactionStateNames[(int)state.Phase], true);
            w.I32(state?.TimeoutMs ?? 0);
            w.I64(state is { Started: > 0 } ? state.Started : -1);
            w.I64(state?.ProducerId ?? -1);
            w.I16(state?.Epoch ?? -1);
            var topics = state?.Partitions.GroupBy(static partition => partition.Topic).ToArray() ?? [];
            w.ArrayLength(topics.Length, true);
            foreach (var topic in topics)
            {
                w.Text(topic.Key, true);
                w.ArrayLength(topic.Count(), true);
                foreach (var partition in topic) w.I32(partition.Partition);
                w.UVar(0);
            }
            w.UVar(0);
        }
        w.UVar(0);
        return true;
    }

    private async ValueTask<bool> ListTransactionsAsync(ReadOnlyMemory<byte> body, WireWriter w, Request request,
        CancellationToken stop)
    {
        short version = request.Version;
        var r = new WireReader(body.Span);
        int count = r.ArrayLength(true);
        bool filterStates = count != 0;
        var unknown = new HashSet<string>(StringComparer.Ordinal);
        var phases = new HashSet<TransactionPhase>();
        for (int i = 0; i < count; i++)
        {
            string name = r.Text(true);
            int index = Array.IndexOf(TransactionStateNames, name);
            if (index >= 0) phases.Add((TransactionPhase)index);
            else if (name is not ("Dead" or "PrepareEpochFence")) unknown.Add(name);
        }
        var producers = new HashSet<long>();
        count = r.ArrayLength(true);
        for (int i = 0; i < count; i++) producers.Add(r.I64());
        long duration = version >= 1 ? r.I64() : -1;
        string? pattern = version >= 2 ? r.NullableText(true) : null;
        r.Tags();
        r.End();

        Error error = Error.None;
        List<(string Name, ClusterTransaction State)> found = [];
        if (cluster.Metadata.IsLeader)
        {
            error = await CoordinatorReadAsync(stop);
            if (error == Error.None)
            {
                long now = BrokerCluster.TransactionTime();
                try
                {
                    var regex = string.IsNullOrEmpty(pattern) ? null : new Regex(@"\A(?:" + pattern + @")\z", RegexOptions.CultureInvariant,
                        TimeSpan.FromMilliseconds(50));
                    foreach (var (name, state) in cluster.Metadata.Image.Transactions)
                    {
                        stop.ThrowIfCancellationRequested();
                        if (request.AllowsTransaction(name, AclOperation.Describe) &&
                            (!filterStates || phases.Contains(state.Phase)) &&
                            (producers.Count == 0 || producers.Contains(state.ProducerId)) &&
                            (duration < 0 || now - (state.Started == 0 ? -1 : state.Started) > duration) &&
                            (regex is null || regex.IsMatch(name)))
                            found.Add((name, state));
                    }
                }
                catch (Exception e) when (e is ArgumentException or RegexMatchTimeoutException)
                {
                    error = Error.InvalidRegularExpression;
                    found.Clear();
                }
            }
        }
        else if (cluster.Metadata.LeaderId < 0) error = Error.CoordinatorNotAvailable;
        w.I32(0);
        w.Write(error);
        w.ArrayLength(unknown.Count, true);
        foreach (string state in unknown) w.Text(state, true);
        w.ArrayLength(found.Count, true);
        foreach (var (name, state) in found)
        {
            w.Text(name, true);
            w.I64(state.ProducerId);
            w.Text(TransactionStateNames[(int)state.Phase], true);
            w.UVar(0);
        }
        w.UVar(0);
        return true;
    }
}
