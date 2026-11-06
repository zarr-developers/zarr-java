package dev.zarr.zarrjava.v3.chunkkeyencoding;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Chunk key encoding that builds keys from a format string such as {@code "images/{-1}/{*:04}.jpeg"}.
 *
 * @see <a href="https://github.com/zarr-developers/zarr-extensions/tree/main/chunk-key-encodings/template">template chunk key encoding</a>
 */
public class TemplateChunkKeyEncoding extends ChunkKeyEncoding {

    private static final Pattern EXPRESSION = Pattern.compile("(\\*|-?\\d+)(?::0(\\d+))?");
    private static final String[] FORBIDDEN_LITERALS = {"..", "\\", "\0", "//", "zarr.json"};

    @JsonIgnore
    public final String name = "template";
    @Nonnull
    public final Configuration configuration;

    @JsonIgnore
    private final List<Part> parts;
    @JsonIgnore
    private final boolean hasCatchAll;

    @JsonCreator
    public TemplateChunkKeyEncoding(
            @Nonnull @JsonProperty(value = "configuration", required = true) Configuration configuration
    ) throws ZarrException {
        this.configuration = configuration;
        this.parts = parse(configuration.format);

        int catchAlls = 0;
        for (Part part : parts) {
            if (part instanceof Expression && ((Expression) part).index == null) {
                catchAlls++;
            }
        }
        if (catchAlls > 1) {
            throw new ZarrException("Template '" + configuration.format + "' contains more than one '{*}' expression.");
        }
        this.hasCatchAll = catchAlls == 1;
        if (hasCatchAll && configuration.separator == null) {
            throw new ZarrException("Template '" + configuration.format + "' contains '{*}' but no separator is configured.");
        }
    }

    public TemplateChunkKeyEncoding(@Nonnull String format, @Nullable String separator) throws ZarrException {
        this(new Configuration(format, separator));
    }

    public TemplateChunkKeyEncoding(@Nonnull String format) throws ZarrException {
        this(format, null);
    }

    private static List<Part> parse(String format) throws ZarrException {
        if (format == null) {
            throw new ZarrException("Template chunk key encoding requires a 'format'.");
        }
        List<Part> parts = new ArrayList<>();
        StringBuilder literal = new StringBuilder();
        int i = 0;
        while (i < format.length()) {
            char c = format.charAt(i);
            boolean doubled = i + 1 < format.length() && format.charAt(i + 1) == c;
            if (c == '{' && doubled || c == '}' && doubled) {
                literal.append(c);
                i += 2;
            } else if (c == '{') {
                int end = format.indexOf('}', i);
                if (end < 0) {
                    throw new ZarrException("Template '" + format + "' has an unmatched '{'.");
                }
                Matcher matcher = EXPRESSION.matcher(format.substring(i + 1, end));
                if (!matcher.matches()) {
                    throw new ZarrException("Template '" + format + "' has an invalid expression '" +
                            format.substring(i, end + 1) + "'.");
                }
                if (literal.length() > 0) {
                    parts.add(new Literal(literal.toString()));
                    literal.setLength(0);
                }
                Integer index = matcher.group(1).equals("*") ? null : Integer.valueOf(matcher.group(1));
                int width = matcher.group(2) == null ? 0 : Integer.parseInt(matcher.group(2));
                parts.add(new Expression(index, width));
                i = end + 1;
            } else if (c == '}') {
                throw new ZarrException("Template '" + format + "' has an unmatched '}'.");
            } else {
                literal.append(c);
                i++;
            }
        }
        if (literal.length() > 0) {
            parts.add(new Literal(literal.toString()));
        }

        if (parts.stream().noneMatch(p -> p instanceof Expression)) {
            throw new ZarrException("Template '" + format + "' needs to contain at least one expression.");
        }
        if (parts.get(0) instanceof Literal && ((Literal) parts.get(0)).value.startsWith("/")) {
            throw new ZarrException("Template '" + format + "' must not begin with '/'.");
        }
        for (Part part : parts) {
            if (part instanceof Literal) {
                for (String forbidden : FORBIDDEN_LITERALS) {
                    if (((Literal) part).value.contains(forbidden)) {
                        throw new ZarrException("Template '" + format + "' must not contain '" + forbidden + "'.");
                    }
                }
            }
        }
        return Collections.unmodifiableList(parts);
    }

    private static String pad(long value, int width) {
        String digits = Long.toString(Math.abs(value));
        String sign = value < 0 ? "-" : "";
        StringBuilder sb = new StringBuilder(sign);
        for (int i = sign.length() + digits.length(); i < width; i++) {
            sb.append('0');
        }
        return sb.append(digits).toString();
    }

    /**
     * Resolves each expression to a position in a chunk coordinate of length {@code ndim}.
     * The catch-all expression resolves to {@code -1}.
     */
    private int[] resolve(int ndim) throws ZarrException {
        int[] positions = new int[parts.size()];
        boolean[] used = new boolean[ndim];
        int explicit = 0;
        for (int p = 0; p < parts.size(); p++) {
            if (!(parts.get(p) instanceof Expression)) {
                continue;
            }
            Integer index = ((Expression) parts.get(p)).index;
            if (index == null) {
                positions[p] = -1;
                continue;
            }
            int position = index < 0 ? ndim + index : index;
            if (position < 0 || position >= ndim) {
                throw new ZarrException("Index " + index + " of template '" + configuration.format +
                        "' is out of range for " + ndim + " dimensions.");
            }
            if (used[position]) {
                throw new ZarrException("Template '" + configuration.format + "' refers to dimension " +
                        position + " more than once.");
            }
            used[position] = true;
            positions[p] = position;
            explicit++;
        }
        if (hasCatchAll && explicit == ndim) {
            throw new ZarrException("Template '" + configuration.format + "' leaves no dimensions for '{*}' with " +
                    ndim + " dimensions.");
        }
        if (!hasCatchAll && explicit != ndim) {
            throw new ZarrException("Template '" + configuration.format + "' has " + explicit +
                    " expressions, but the array has " + ndim + " dimensions.");
        }
        return positions;
    }

    @Override
    public void validate(int ndim) throws ZarrException {
        resolve(ndim);
    }

    public String encode(long[] chunkCoords) throws ZarrException {
        int[] positions = resolve(chunkCoords.length);
        boolean[] used = new boolean[chunkCoords.length];
        for (int p = 0; p < parts.size(); p++) {
            if (parts.get(p) instanceof Expression && positions[p] >= 0) {
                used[positions[p]] = true;
            }
        }
        StringBuilder key = new StringBuilder();
        for (int p = 0; p < parts.size(); p++) {
            Part part = parts.get(p);
            if (part instanceof Literal) {
                key.append(((Literal) part).value);
                continue;
            }
            int width = ((Expression) part).width;
            if (positions[p] >= 0) {
                key.append(pad(chunkCoords[positions[p]], width));
                continue;
            }
            boolean first = true;
            for (int d = 0; d < chunkCoords.length; d++) {
                if (!used[d]) {
                    if (!first) {
                        key.append(configuration.separator);
                    }
                    key.append(pad(chunkCoords[d], width));
                    first = false;
                }
            }
        }
        return key.toString();
    }

    @Override
    public String[] encodeChunkKey(long[] chunkCoords) {
        try {
            return encode(chunkCoords).split("/");
        } catch (ZarrException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    private interface Part {
    }

    private static final class Literal implements Part {
        final String value;

        Literal(String value) {
            this.value = value;
        }
    }

    private static final class Expression implements Part {
        /** Index into the chunk coordinate, or {@code null} for the catch-all {@code {*}}. */
        final Integer index;
        final int width;

        Expression(Integer index, int width) {
            this.index = index;
            this.width = width;
        }
    }

    public static final class Configuration {

        @Nonnull
        public final String format;
        @Nullable
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public final String separator;

        @JsonCreator
        public Configuration(
                @Nonnull @JsonProperty(value = "format", required = true) String format,
                @Nullable @JsonProperty(value = "separator") String separator) {
            this.format = format;
            this.separator = separator;
        }
    }
}
