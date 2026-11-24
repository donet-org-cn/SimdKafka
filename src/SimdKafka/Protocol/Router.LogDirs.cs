namespace SimdKafka;

internal sealed partial class Router
{
    private void DescribeLogDirs(ref WireReader r, WireWriter w, Request request)
    {
        var selection = ReadLogDirSelection(ref r, request.Flexible);
        if (!request.AllowsCluster(AclOperation.Describe))
        {
            WriteLogDirsUnauthorized(w, request);
            return;
        }
        var reported = new List<(string Name, List<(int Index, long Size, long Lag)> Partitions)>();
        foreach (var topic in catalog.Topics())
        {
            if (selection is not null && !selection.ContainsKey(topic.Name)) continue;
            var selected = selection?.GetValueOrDefault(topic.Name);
            var partitions = new List<(int Index, long Size, long Lag)>();
            for (int i = 0; i < topic.Partitions.Length; i++)
                if (selected is null || selected.Contains(i)) partitions.Add((i, topic.Partitions[i].EncodedLogBytes, 0));
            if (partitions.Count != 0) reported.Add((topic.Name, partitions));
        }
        WriteLogDirs(w, request, options.Data, options.Data, reported);
    }

    internal static Dictionary<string, HashSet<int>>? ReadLogDirSelection(ref WireReader r, bool compact)
    {
        int count = r.ArrayLength(compact, nullable: true);
        var selection = count < 0 ? null : new Dictionary<string, HashSet<int>>(StringComparer.Ordinal);
        for (int i = 0; i < count; i++)
        {
            string name = r.Text(compact);
            int partitions = r.ArrayLength(compact);
            if (!selection!.TryGetValue(name, out var selected)) selection[name] = selected = [];
            for (int p = 0; p < partitions; p++) selected.Add(r.I32());
            r.EndStruct(compact);
        }
        r.EndStruct(compact); r.End();
        return selection;
    }

    internal static void WriteLogDirsUnauthorized(WireWriter w, Request request)
    {
        w.I32(0);
        if (request.Version >= 3) w.Write(Error.ClusterAuthorizationFailed);
        w.ArrayLength(0, request.Flexible);
        w.EndStruct(request.Flexible);
    }

    internal static void WriteLogDirs(WireWriter w, Request request, string dataRoot, string logDirectory,
        List<(string Name, List<(int Index, long Size, long Lag)> Partitions)> topics)
    {
        bool compact = request.Flexible;
        w.I32(0);
        if (request.Version >= 3) w.Write(Error.None);
        w.ArrayLength(1, compact);
        w.Write(Directory.Exists(dataRoot) ? Error.None : Error.KafkaStorageError);
        w.Text(logDirectory, compact);
        w.ArrayLength(topics.Count, compact);
        foreach (var (name, partitions) in topics)
        {
            w.Text(name, compact);
            w.ArrayLength(partitions.Count, compact);
            foreach (var (index, size, lag) in partitions)
            {
                w.I32(index); w.I64(size); w.I64(lag); w.Bool(false);
                w.EndStruct(compact);
            }
            w.EndStruct(compact);
        }
        if (request.Version >= 4)
        {
            var (total, usable) = VolumeCapacity(dataRoot);
            w.I64(total);
            w.I64(usable);
        }
        w.EndStruct(compact); w.EndStruct(compact);
    }

    private static (long Total, long Usable) VolumeCapacity(string path)
    {
        try
        {
            var drive = DriveInfo.GetDrives().Where(disk =>
            {
                string relative = Path.GetRelativePath(disk.Name, path);
                return !Path.IsPathRooted(relative) && relative != ".." &&
                    !relative.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal);
            }).MaxBy(static disk => disk.Name.Length);
            if (drive is { IsReady: true }) return (drive.TotalSize, drive.AvailableFreeSpace);
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException) { }
        return (-1, -1);
    }
}
