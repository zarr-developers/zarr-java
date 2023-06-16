package com.scalableminds.zarrjava.indexing;

public class OpenSlice {
    public Long start;
    public Long end;

    public OpenSlice(long start, long end) {
        this.start = start;
        this.end = end;
    }

    public StrictSlice normalize(long length) {
        long _start = 0;
        long _end = 0;
        if (start != null) {
            _start = start;
            if (_start < 0) {
                _start += length;
                if (_start < 0) {
                    _start = 0;
                }
            }
        }
        if (end != null) {
            _end = end;
            if (_end < 0) {
                _end += length;
                if (_end < 0) {
                    _end = 0;
                }
            }
        }
        return new StrictSlice(_start, _end);
    }

}
