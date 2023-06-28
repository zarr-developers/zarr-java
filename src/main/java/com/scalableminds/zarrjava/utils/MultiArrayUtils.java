package com.scalableminds.zarrjava.utils;

import java.util.ArrayList;
import java.util.Arrays;
import javax.annotation.Nonnull;
import ucar.ma2.Array;
import ucar.ma2.IndexIterator;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;

public class MultiArrayUtils {

  public static void copyRegion(Array source, int[] sourceOffset, Array target, int[] targetOffset,
      int[] shape) {
    if (sourceOffset.length != targetOffset.length) {
      throw new IllegalArgumentException(
          "'sourceOffset' and 'targetOffset' do not have the same rank.");
    }
    if (source.getRank() != sourceOffset.length) {
      throw new IllegalArgumentException("'sourceOffset' and 'source' do not have the same rank.");
    }
    if (target.getRank() != targetOffset.length) {
      throw new IllegalArgumentException("'targetOffset' and 'target' do not have the same rank.");
    }
    if (shape.length != sourceOffset.length) {
      throw new IllegalArgumentException("'shape' and 'sourceOffset' do not have the same rank.");
    }

    try {
      final ArrayList<Range> sourceRanges = new ArrayList<>();
      final ArrayList<Range> targetRanges = new ArrayList<>();
      for (int dimIdx = 0; dimIdx < shape.length; dimIdx++) {
        if (sourceOffset[dimIdx] + shape[dimIdx] > source.getShape()[dimIdx]) {
          throw new IllegalArgumentException(
              "'sourceOffset + shape' needs to be less or equal than " + "'source.getShape()'.");
        }
        if (targetOffset[dimIdx] + shape[dimIdx] > target.getShape()[dimIdx]) {
          throw new IllegalArgumentException(
              "'targetOffset + shape' needs to be less or equal than " + "'target.getShape()'.");
        }

        sourceRanges.add(new Range(sourceOffset[dimIdx], sourceOffset[dimIdx] + shape[dimIdx] - 1));
        targetRanges.add(new Range(targetOffset[dimIdx], targetOffset[dimIdx] + shape[dimIdx] - 1));
      }
      final IndexIterator sourceRangeIterator = source.getRangeIterator(sourceRanges);
      final IndexIterator targetRangeIterator = target.getRangeIterator(targetRanges);
      final Class elementType = source.getElementType();
      final ValueAccessor accessor = createValueAccessor(elementType);

      while (sourceRangeIterator.hasNext()) {
        accessor.copy(sourceRangeIterator, targetRangeIterator);
      }
    } catch (InvalidRangeException ex) {
      throw new RuntimeException("Unreachable");
    }
  }

  public static Array fill(@Nonnull Array array, @Nonnull Object fillValue) {
    IndexIterator iterator = array.getIndexIterator();
    final Class elementType = array.getElementType();
    final ValueAccessor accessor = createValueAccessor(elementType);
    while (iterator.hasNext()) {
      accessor.set(iterator, fillValue);
    }
    return array;
  }

  public static boolean allValuesEqual(Array array, Object value) {
    IndexIterator iterator = array.getIndexIterator();
    final Class elementType = array.getElementType();
    final ValueAccessor accessor = createValueAccessor(elementType);
    while (iterator.hasNext()) {
      boolean isEqual = accessor.isEqual(iterator, value);
      if (!isEqual) {
        return false;
      }
    }
    return true;
  }

  public static boolean allValuesEqual(Array source, Array target) {
    if (!Arrays.equals(source.getShape(), target.getShape())) {
      return false;
    }
    if (!source.getElementType()
        .equals(target.getElementType())) {
      return false;
    }

    IndexIterator sourceIterator = source.getIndexIterator();
    IndexIterator targetIterator = target.getIndexIterator();
    final Class elementType = source.getElementType();
    final ValueAccessor accessor = createValueAccessor(elementType);
    while (sourceIterator.hasNext()) {
      boolean isEqual = accessor.isEqual(sourceIterator, targetIterator);
      if (!isEqual) {
        return false;
      }
    }
    return true;
  }

  static ValueAccessor createValueAccessor(Class elementType) {
    if (elementType == double.class) {
      return new ValueAccessor() {
        @Override
        public void copy(IndexIterator sourceIterator, IndexIterator targetIterator) {
          targetIterator.setDoubleNext(sourceIterator.getDoubleNext());
        }

        @Override
        public void set(IndexIterator iterator, Object value) {
          iterator.setDoubleNext((double) value);
        }

        @Override
        public boolean isEqual(IndexIterator iterator, Object value) {
          return iterator.getDoubleNext() == (double) value;
        }

        @Override
        public boolean isEqual(IndexIterator sourceIterator, IndexIterator targetIterator) {
          return targetIterator.getDoubleNext() == sourceIterator.getDoubleNext();
        }
      };
    } else if (elementType == float.class) {
      return new ValueAccessor() {
        @Override
        public void copy(IndexIterator sourceIterator, IndexIterator targetIterator) {
          targetIterator.setFloatNext(sourceIterator.getFloatNext());
        }

        @Override
        public void set(IndexIterator iterator, Object value) {
          iterator.setFloatNext((float) value);
        }

        @Override
        public boolean isEqual(IndexIterator iterator, Object value) {
          return iterator.getFloatNext() == (float) value;
        }

        @Override
        public boolean isEqual(IndexIterator sourceIterator, IndexIterator targetIterator) {
          return targetIterator.getFloatNext() == sourceIterator.getFloatNext();
        }
      };
    } else if (elementType == long.class) {
      return new ValueAccessor() {
        @Override
        public void copy(IndexIterator sourceIterator, IndexIterator targetIterator) {
          targetIterator.setLongNext(sourceIterator.getLongNext());
        }

        @Override
        public void set(IndexIterator iterator, Object value) {
          iterator.setLongNext((long) value);
        }

        @Override
        public boolean isEqual(IndexIterator iterator, Object value) {
          return iterator.getLongNext() == (long) value;
        }

        @Override
        public boolean isEqual(IndexIterator sourceIterator, IndexIterator targetIterator) {
          return targetIterator.getLongNext() == sourceIterator.getLongNext();
        }
      };
    } else if (elementType == int.class) {
      return new ValueAccessor() {
        @Override
        public void copy(IndexIterator sourceIterator, IndexIterator targetIterator) {
          targetIterator.setIntNext(sourceIterator.getIntNext());
        }

        @Override
        public void set(IndexIterator iterator, Object value) {
          iterator.setIntNext((int) value);
        }

        @Override
        public boolean isEqual(IndexIterator iterator, Object value) {
          return iterator.getIntNext() == (int) value;
        }

        @Override
        public boolean isEqual(IndexIterator sourceIterator, IndexIterator targetIterator) {
          return targetIterator.getIntNext() == sourceIterator.getIntNext();
        }
      };
    } else if (elementType == short.class) {
      return new ValueAccessor() {
        @Override
        public void copy(IndexIterator sourceIterator, IndexIterator targetIterator) {
          targetIterator.setShortNext(sourceIterator.getShortNext());
        }

        @Override
        public void set(IndexIterator iterator, Object value) {
          iterator.setShortNext((short) value);
        }

        @Override
        public boolean isEqual(IndexIterator iterator, Object value) {
          return iterator.getShortNext() == (short) value;
        }

        @Override
        public boolean isEqual(IndexIterator sourceIterator, IndexIterator targetIterator) {
          return targetIterator.getShortNext() == sourceIterator.getShortNext();
        }
      };
    } else if (elementType == byte.class) {
      return new ValueAccessor() {
        @Override
        public void copy(IndexIterator sourceIterator, IndexIterator targetIterator) {
          targetIterator.setByteNext(sourceIterator.getByteNext());
        }

        @Override
        public void set(IndexIterator iterator, Object value) {
          iterator.setByteNext((byte) value);
        }

        @Override
        public boolean isEqual(IndexIterator iterator, Object value) {
          return iterator.getByteNext() == (byte) value;
        }

        @Override
        public boolean isEqual(IndexIterator sourceIterator, IndexIterator targetIterator) {
          return targetIterator.getByteNext() == sourceIterator.getByteNext();
        }
      };
    } else if (elementType == boolean.class) {
      return new ValueAccessor() {
        @Override
        public void copy(IndexIterator sourceIterator, IndexIterator targetIterator) {
          targetIterator.setBooleanNext(sourceIterator.getBooleanNext());
        }

        @Override
        public void set(IndexIterator iterator, Object value) {
          iterator.setBooleanNext((boolean) value);
        }

        @Override
        public boolean isEqual(IndexIterator iterator, Object value) {
          return iterator.getBooleanNext() == (boolean) value;
        }

        @Override
        public boolean isEqual(IndexIterator sourceIterator, IndexIterator targetIterator) {
          return targetIterator.getBooleanNext() == sourceIterator.getBooleanNext();
        }
      };
    }
    return new ValueAccessor() {
      @Override
      public void copy(IndexIterator sourceIterator, IndexIterator targetIterator) {
        targetIterator.setObjectNext(sourceIterator.getObjectNext());
      }

      @Override
      public void set(IndexIterator iterator, Object value) {
        iterator.setObjectNext(value);
      }

      @Override
      public boolean isEqual(IndexIterator iterator, Object value) {
        return iterator.getObjectNext()
            .equals(value);
      }

      @Override
      public boolean isEqual(IndexIterator sourceIterator, IndexIterator targetIterator) {
        return targetIterator.getObjectNext()
            .equals(sourceIterator.getObjectNext());
      }
    };
  }

  private interface ValueAccessor {

    void copy(IndexIterator sourceIterator, IndexIterator targetIterator);

    void set(IndexIterator iterator, Object value);

    boolean isEqual(IndexIterator iterator, Object value);

    boolean isEqual(IndexIterator sourceIterator, IndexIterator targetIterator);

  }
}
