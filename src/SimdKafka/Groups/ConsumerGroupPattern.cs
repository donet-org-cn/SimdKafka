using System.Text;
using System.Text.RegularExpressions;

namespace SimdKafka;

internal static class ConsumerGroupPattern
{
    private const int MaxPatternLength = 2_048;

    internal static bool TryCompile(string pattern, out Regex? matcher)
    {
        matcher = null;
        if (pattern.Length is 0 or > MaxPatternLength) return false;
        try
        {
            matcher = new Regex(@"\A(?:" + Translate(pattern) + @")\z",
                RegexOptions.CultureInvariant | RegexOptions.NonBacktracking | RegexOptions.ExplicitCapture);
            return true;
        }
        catch (Exception error) when (error is ArgumentException or NotSupportedException or FormatException)
        {
            return false;
        }
    }

    internal static Regex Compile(string pattern) => TryCompile(pattern, out var matcher) ? matcher! :
        throw new InvalidDataException("Invalid persisted consumer group subscription pattern");

    private static string Translate(string pattern)
    {
        var regex = new StringBuilder(pattern.Length);
        bool inClass = false, firstInClass = false;
        for (int i = 0; i < pattern.Length; i++)
        {
            char ch = pattern[i];
            if (ch == '\\')
            {
                if (++i == pattern.Length) throw new FormatException();
                AppendEscape(regex, pattern, ref i);
            }
            else if (ch == '[' && inClass)
            {
                if (pattern.AsSpan(i).StartsWith("[:"))
                {
                    int end = pattern.IndexOf(":]", i + 2, StringComparison.Ordinal);
                    if (end < 0) throw new FormatException();
                    regex.Append(PosixClass(pattern[(i + 2)..end]));
                    i = end + 1;
                }
                else regex.Append(@"\[");
            }
            else if (ch == '[')
            {
                inClass = firstInClass = true;
                regex.Append(ch);
                if (i + 1 < pattern.Length && pattern[i + 1] == '^') regex.Append(pattern[++i]);
                continue;
            }
            else if (ch == ']' && inClass)
            {
                if (firstInClass) regex.Append(@"\]");
                else { regex.Append(ch); inClass = false; }
            }
            else if (!inClass && ch == '(' && i + 1 < pattern.Length && pattern[i + 1] == '?')
                AppendGroup(regex, pattern, ref i);
            else regex.Append(ch);
            firstInClass = false;
        }
        return regex.ToString();
    }

    private static void AppendEscape(StringBuilder regex, string pattern, ref int i)
    {
        char escape = pattern[i];
        if (escape == 'Q')
        {
            int end = pattern.IndexOf(@"\E", i + 1, StringComparison.Ordinal);
            if (end < 0) end = pattern.Length;
            for (int p = i + 1; p < end; p++) regex.Append(@"\u").Append(((int)pattern[p]).ToString("X4"));
            i = end == pattern.Length ? end - 1 : end + 1;
            return;
        }
        if (escape == 'x')
        {
            int start = i + 1, end = start + 2;
            if (start < pattern.Length && pattern[start] == '{')
            {
                end = pattern.IndexOf('}', ++start);
                if (end < 0) throw new FormatException();
                i = end;
            }
            else i += 2;
            if (end > pattern.Length || end - start is < 1 or > 6 ||
                !int.TryParse(pattern.AsSpan(start, end - start), System.Globalization.NumberStyles.AllowHexSpecifier,
                    System.Globalization.CultureInfo.InvariantCulture, out int value) || !Rune.IsValid(value))
                throw new FormatException();
            foreach (char ch in char.ConvertFromUtf32(value)) regex.Append(@"\u").Append(((int)ch).ToString("X4"));
            return;
        }
        if (escape is >= '0' and <= '7')
        {
            int first = i, octal = escape - '0';
            while (i + 1 < pattern.Length && i - first < 2 && pattern[i + 1] is >= '0' and <= '7')
                octal = octal * 8 + pattern[++i] - '0';
            if (i == first && escape != '0') throw new FormatException();
            regex.Append(@"\u").Append(octal.ToString("X4"));
            return;
        }
        if ("AbBzdDsSwWpPafnrtv".Contains(escape) || escape is >= '!' and <= '~' && !char.IsAsciiLetterOrDigit(escape))
            regex.Append('\\').Append(escape);
        else throw new FormatException();
    }

    private static void AppendGroup(StringBuilder regex, string pattern, ref int i)
    {
        int start = i + 2;
        var tail = pattern.AsSpan(start);
        int prefix = tail.StartsWith("P<") ? 2 : tail.StartsWith("<") ? 1 : 0;
        if (prefix > 0)
        {
            start += prefix;
            int end = pattern.IndexOf('>', start);
            if (end <= start || !char.IsAsciiLetter(pattern[start]) && pattern[start] != '_' ||
                pattern.AsSpan(start, end - start).ContainsAnyExcept("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"))
                throw new FormatException();
            regex.Append("(?:");
            i = end;
            return;
        }
        int option = start;
        while (option < pattern.Length && "imsU-".Contains(pattern[option])) option++;
        if (option == pattern.Length || pattern[option] is not (')' or ':') || option == start && pattern[option] == ')')
            throw new FormatException();
        string flags = pattern[start..option].Replace("U", "", StringComparison.Ordinal).TrimEnd('-');
        if (flags.Length == 0) regex.Append(pattern[option] == ':' ? "(?:" : "(?:)");
        else regex.Append("(?").Append(flags).Append(pattern[option]);
        i = option;
    }

    private static string PosixClass(string name) => name switch
    {
        "alnum" => "0-9A-Za-z", "alpha" => "A-Za-z", "ascii" => @"\u0000-\u007f", "blank" => @" \t",
        "cntrl" => @"\u0000-\u001f\u007f", "digit" => "0-9", "graph" => @"\u0021-\u007e",
        "lower" => "a-z", "print" => @"\u0020-\u007e", "punct" => @"\u0021-\u002f\u003a-\u0040\u005b-\u0060\u007b-\u007e",
        "space" => @"\t-\r ", "upper" => "A-Z", "word" => "0-9A-Z_a-z", "xdigit" => "0-9A-Fa-f",
        _ => throw new FormatException()
    };
}
