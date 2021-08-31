import vaex
import pyarrow
import pyarrow as pa
import numpy as np

import enum
#import collections
import ctypes
from typing import Any, Optional, Tuple, Dict, Iterable, Sequence

DataFrameObject = Any
ColumnObject = Any

def from_dataframe_to_vaex(df : DataFrameObject) -> vaex.dataframe.DataFrame:
    """
    Construct a vaex DataFrame from ``df`` if it supports ``__dataframe__``
    """
    # NOTE: commented out for roundtrip testing
    # if isinstance(df, vaex.dataframe.DataFrame):
    #     return df

    if not hasattr(df, '__dataframe__'):
        raise ValueError("`df` does not support __dataframe__")

    return _from_dataframe_to_vaex(df.__dataframe__())

def _from_dataframe_to_vaex(df : DataFrameObject) -> vaex.dataframe.DataFrame:
    """
    Note: not all cases are handled yet, only ones that can be implemented with
    only Pandas. Later, we need to implement/test support for categoricals,
    bit/byte masks, chunk handling, etc.
    """
    # Check number of chunks, if there's more than one we need to iterate
    # Alenka: it is set to 1 anyways??
    if df.num_chunks() > 1:
        raise NotImplementedError

    # We need a dict of columns here, with each column being a numpy array (at
    # least for now, deal with non-numpy/arrow dtypes later).
    columns = dict()
    _k = _DtypeKind
    for name in df.column_names():
        col = df.get_column_by_name(name)
        if col.dtype[0] in (_k.INT, _k.UINT, _k.FLOAT, _k.BOOL):
            # Simple numerical or bool dtype, turn into numpy array
            columns[name] = convert_column_to_ndarray(col)
        #elif col.dtype[0] == _k.CATEGORICAL:
        #    columns[name] = convert_categorical_column(col)
        else:
            raise NotImplementedError(f"Data type {col.dtype[0]} not handled yet")

    return vaex.from_dict(columns)

class _DtypeKind(enum.IntEnum):
    INT = 0
    UINT = 1
    FLOAT = 2
    BOOL = 20
    STRING = 21   # UTF-8
    DATETIME = 22
    CATEGORICAL = 23
    
def convert_column_to_ndarray(col : ColumnObject) -> np.ndarray:
    """
    Convert an int, uint, float or bool column to a numpy array
    """
    if col.offset != 0:
        raise NotImplementedError("column.offset > 0 not handled yet")

    if col.describe_null[0] not in (0, 1):
        raise NotImplementedError("Null values represented as masks or "
                                  "sentinel values not handled yet")

    _buffer, _dtype = col.get_data_buffer()
    return buffer_to_ndarray(_buffer, _dtype)

def buffer_to_ndarray(_buffer, _dtype) -> np.ndarray:
    # Handle the dtype
    kind = _dtype[0]
    bitwidth = _dtype[1]
    _k = _DtypeKind
    if _dtype[0] not in (_k.INT, _k.UINT, _k.FLOAT, _k.BOOL):
        raise RuntimeError("Not a boolean, integer or floating-point dtype")

    _ints = {8: np.int8, 16: np.int16, 32: np.int32, 64: np.int64}
    _uints = {8: np.uint8, 16: np.uint16, 32: np.uint32, 64: np.uint64}
    _floats = {32: np.float32, 64: np.float64}
    _np_dtypes = {0: _ints, 1: _uints, 2: _floats, 20: {8: bool}}
    column_dtype = _np_dtypes[kind][bitwidth]

    # No DLPack yet, so need to construct a new ndarray from the data pointer
    # and size in the buffer plus the dtype on the column
    ctypes_type = np.ctypeslib.as_ctypes_type(column_dtype)
    data_pointer = ctypes.cast(_buffer.ptr, ctypes.POINTER(ctypes_type))

    # NOTE: `x` does not own its memory, so the caller of this function must
    #       either make a copy or hold on to a reference of the column or
    #       buffer! (not done yet, this is pretty awful ...)
    x = np.ctypeslib.as_array(data_pointer,
                              shape=(_buffer.bufsize // (bitwidth//8),))

    return x

def __dataframe__(cls, nan_as_null : bool = False) -> dict:
    """
    The public method to attach to pd.DataFrame
    We'll attach it via monkeypatching here for demo purposes. If vaex adopt
    the protocol, this will be a regular method on vaex.dataframe.DataFrame.
    ``nan_as_null`` is a keyword intended for the consumer to tell the
    producer to overwrite null values in the data with ``NaN`` (or ``NaT``).
    This currently has no effect; once support for nullable extension
    dtypes is added, this value should be propagated to columns.
    """
    return _VaexDataFrame(cls, nan_as_null=nan_as_null)

# Monkeypatch the Vaex DataFrame class to support the interchange protocol
vaex.dataframe.DataFrame.__dataframe__ = __dataframe__

# Implementation of interchange protocol
# --------------------------------------

class _VaexBuffer:
    """
    Data in the buffer is guaranteed to be contiguous in memory.
    """

    def __init__(self, x : np.ndarray) -> None:
        """
        Handle only regular columns (= numpy arrays) for now.
        """
        # Store the numpy array in which the data resides as a private
        # attribute, so we can use it to retrieve the public attributes
        self._x = x
    
    @property
    def bufsize(self) -> int:
        """
        Buffer size in bytes
        """
        return self._x.size * self._x.dtype.itemsize
    
    @property
    def ptr(self) -> int:
        """
        Pointer to start of the buffer as an integer
        """
        return self._x.__array_interface__['data'][0]

class _VaexColumn:
    """
    A column object, with only the methods and properties required by the
    interchange protocol defined.
    A column can contain one or more chunks. Each chunk can contain either one
    or two buffers - one data buffer and (depending on null representation) it
    may have a mask buffer.
    Note: this Column object can only be produced by ``__dataframe__``, so
          doesn't need its own version or ``__column__`` protocol.
    """

    def __init__(self, column : vaex.expression.Expression) -> None:
        """
        Note: doesn't deal with extension arrays yet, just assume an 
        expression for now.
        """
        if not isinstance(column, vaex.expression.Expression):
            raise NotImplementedError("Columns of type {} not handled "
                                      "yet".format(type(column)))

        # Store the column as a private attribute
        self._col = column
        
    @property
    def offset(self) -> int:
        """
        Offset of first element. Always zero.
        """
        return 0
        
    @property
    def dtype(self) -> Tuple[enum.IntEnum, int, str, str]:
        """
        Dtype description as a tuple ``(kind, bit-width, format string, endianness)``
        Kind :
            - INT = 0
            - UINT = 1
            - FLOAT = 2
            - BOOL = 20
            - STRING = 21   # UTF-8
            - DATETIME = 22
            - CATEGORICAL = 23
        Bit-width : the number of bits as an integer
        Format string : data type description format string in Apache Arrow C
                        Data Interface format.
        Endianness : current only native endianness (``=``) is supported
        Notes:
            - Kind specifiers are aligned with DLPack where possible (hence the
              jump to 20, leave enough room for future extension)
            - Masks must be specified as boolean with either bit width 1 (for bit
              masks) or 8 (for byte masks).
            - Dtype width in bits was preferred over bytes
            - Endianness isn't too useful, but included now in case in the future
              we need to support non-native endianness
            - Went with Apache Arrow format strings over NumPy format strings
              because they're more complete from a dataframe perspective
            - Format strings are mostly useful for datetime specification, and
              for categoricals.
            - For categoricals, the format string describes the type of the
              categorical in the data buffer. In case of a separate encoding of
              the categorical (e.g. an integer to string mapping), this can
              be derived from ``self.describe_categorical``.
            - Data types not included: complex, Arrow-style null, binary, decimal,
              and nested (list, struct, map, union) dtypes.
        """
        dtype = self._col.dtype
        return self._dtype_from_vaexdtype(dtype)
    
    def _dtype_from_vaexdtype(self, dtype) -> Tuple[enum.IntEnum, int, str, str]:
        """
        See `self.dtype` for details
        """
        # ?? How about Vaex ??
        # Note: 'c' (complex) not handled yet (not in array spec v1).
        #       'b', 'B' (bytes), 'S', 'a', (old-style string) 'V' (void) not handled
        #       datetime and timedelta both map to datetime (is timedelta handled?)
        _k = _DtypeKind
        _np_kinds = {'i': _k.INT, 'u': _k.UINT, 'f': _k.FLOAT, 'b': _k.BOOL,
                     'U': _k.STRING,
                     'M': _k.DATETIME, 'm': _k.DATETIME}
        kind = _np_kinds.get(dtype.kind, None)
        if kind is None:
            # Not a NumPy dtype. Check if it's a categorical maybe
            if isinstance(dtype, pd.CategoricalDtype):
                kind = 23
            else:
                raise ValueError(f"Data type {dtype} not supported by exchange"
                                 "protocol")

        if kind not in (_k.INT, _k.UINT, _k.FLOAT, _k.BOOL, _k.CATEGORICAL):
            raise NotImplementedError(f"Data type {dtype} not handled yet")

        bitwidth = dtype.numpy.itemsize * 8
        format_str = dtype.numpy.str
        endianness = dtype.byteorder if not kind == _k.CATEGORICAL else '='
        return (kind, bitwidth, format_str, endianness)
    
    @property
    def describe_null(self) -> Tuple[int, Any]:
        """
        Return the missing value (or "null") representation the column dtype
        uses, as a tuple ``(kind, value)``.
        Kind:
            - 0 : non-nullable
            - 1 : NaN/NaT
            - 2 : sentinel value
            - 3 : bit mask
            - 4 : byte mask
        Value : if kind is "sentinel value", the actual value. None otherwise.
        """
        _k = _DtypeKind
        kind = self.dtype[0]
        value = None
        if kind == _k.FLOAT:
            null = 1  # np.nan
        elif kind == _k.DATETIME:
            null = 1  # np.datetime64('NaT')
        elif kind in (_k.INT, _k.UINT, _k.BOOL):
            # TODO: check if extension dtypes are used once support for them is
            #       implemented in this procotol code
            null = 0  # integer and boolean dtypes are non-nullable
        elif kind == _k.CATEGORICAL:
            # Null values for categoricals are stored as `-1` sentinel values
            # in the category date (e.g., `col.values.codes` is int8 np.ndarray)
            null = 2
            value = -1
        else:
            raise NotImplementedError(f'Data type {self.dtype} not yet supported')

        return null, value
    
    def get_data_buffer(self) -> Tuple[_VaexBuffer, Any]:  # Any is for self.dtype tuple
        """
        Return the buffer containing the data.
        """
        _k = _DtypeKind
        if self.dtype[0] in (_k.INT, _k.UINT, _k.FLOAT, _k.BOOL):
            buffer = _VaexBuffer(self._col.to_numpy())
            dtype = self.dtype
        elif self.dtype[0] == _k.CATEGORICAL:
            codes = self._col.values.codes
            buffer = _VaexBuffer(codes)
            dtype = self._dtype_from_pandasdtype(codes.dtype)
        else:
            raise NotImplementedError(f"Data type {self._col.dtype} not handled yet")

        return buffer, dtype
    
class _VaexDataFrame:
    """
    A data frame class, with only the methods required by the interchange
    protocol defined.
    Instances of this (private) class are returned from
    ``vaex.dataframe.DataFrame.__dataframe__`` as objects with the methods and
    attributes defined on this class.
    """
    def __init__(self, df : vaex.dataframe.DataFrame, nan_as_null : bool = False) -> None:
        """
        Constructor - an instance of this (private) class is returned from
        `vaex.dataframe.DataFrame.__dataframe__`.
        """
        self._df = df
        # ``nan_as_null`` is a keyword intended for the consumer to tell the
        # producer to overwrite null values in the data with ``NaN`` (or ``NaT``).
        # This currently has no effect; once support for nullable extension
        # dtypes is added, this value should be propagated to columns.
        self._nan_as_null = nan_as_null
        
    def num_chunks(self) -> int:
        return 1
        
    def column_names(self) -> Iterable[str]:
        return self._df.get_column_names()
    
    def get_column_by_name(self, name: str) -> _VaexColumn:
        return _VaexColumn(self._df[name])