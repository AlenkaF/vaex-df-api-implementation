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
    lable = dict()
    _k = _DtypeKind
    for name in df.column_names():
        col = df.get_column_by_name(name)
        if col.dtype[0] in (_k.INT, _k.UINT, _k.FLOAT, _k.BOOL):
            # Simple numerical or bool dtype, turn into numpy array
            columns[name] = convert_column_to_ndarray(col)
        elif col.dtype[0] == _k.CATEGORICAL:
            values, categories = convert_categorical_column(col)
            columns[name] = values
            lable[name] = categories
        else:
            raise NotImplementedError(f"Data type {col.dtype[0]} not handled yet")
    
    dataframe = vaex.from_dict(columns)
    for cat in lable:
        dataframe = dataframe.categorize(cat, labels = lable[cat])
            
    return dataframe

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

def convert_categorical_column(col : ColumnObject) -> Tuple[np.ndarray, np.ndarray]:
    """
    Convert a categorical column to a npdarray ???
    """
    ordered, is_dict, mapping = col.describe_categorical
    if not is_dict:
        raise NotImplementedError('Non-dictionary categoricals not supported yet')
    
    categories = np.asarray(list(mapping.values()))
    buffer, dtype = col.get_data_buffer()
    values = buffer_to_ndarray(buffer, dtype)

    # Seems like Pandas can only construct with non-null values, so need to
    # null out the nulls later
    #cat = pd.Categorical(values, categories=categories, ordered=ordered)
    
    
    # TUKAJ MORAM ZAPISATI VREDNOSTI IN SHRANITI INFO, DA JE TO KATERGORIČNA + LABELS
    # ŠELE POTEM, KO SHRANIMO NAZAJ V VAEX DATAFRAME LAHKO KATEGORIZIRAM STOLPEC!
    #series = pd.Series(cat)
    
    
    #null_kind = col.describe_null[0]
    #if null_kind == 2:  # sentinel value
    #    sentinel = col.describe_null[1]
    #    series[codes == sentinel] = np.nan
    #else:
    #    raise NotImplementedError("Only categorical columns with sentinel "
    #                              "value supported at the moment")

    
    
    return (values, categories)

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
    
    def __dlpack__(self):
        """
        DLPack not implemented in Vaex, so leave it out here
        """
        raise NotImplementedError("__dlpack__")

    def __dlpack_device__(self) -> Tuple[enum.IntEnum, int]:
        """
        Device type and device ID for where the data in the buffer resides.
        """
        class Device(enum.IntEnum):
            CPU = 1

        return (Device.CPU, None)

    def __repr__(self) -> str:
        return 'VaexBuffer(' + str({'bufsize': self.bufsize,
                                      'ptr': self.ptr,
                                      'device': self.__dlpack_device__()[0].name}
                                      ) + ')'


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

    def __init__(self, column : vaex.expression.Expression, metadata : dict = {}) -> None:
        """
        Note: doesn't deal with extension arrays yet, just assume an 
        expression for now.
        IS IT APPLICABLE FOR VAEX?
        """
        if not isinstance(column, vaex.expression.Expression):
            raise NotImplementedError("Columns of type {} not handled "
                                      "yet".format(type(column)))

        # Store the column as a private attribute
        self._col = column
        
        # Store the info about category
        self.is_cat = metadata["vaex.cetagories_bool"][self._col.expression]
        self.labels = metadata["vaex.cetagories"][self._col.expression]
    
    @property
    def size(self) -> int:
        """
        Size of the column, in elements.
        """
        return self._col.values.size
    
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
        # Define how _dtype_from_vaexdtype calculates kind
        # If it is internal, categorical must stay categorical (info in metadata)
        # If it is external (call from_dataframe) must give data dtype
        
        bool_c = False
        if self.is_cat:
            bool_c = True # we want dtype to be categorical

        dtype = self._col.dtype
            
        return self._dtype_from_vaexdtype(dtype, bool_c)
    
    def _dtype_from_vaexdtype(self, dtype, bool_c) -> Tuple[enum.IntEnum, int, str, str]:
        """
        See `self.dtype` for details
        """
        # Note: 'c' (complex) not handled yet (not in array spec v1).
        #       'b', 'B' (bytes), 'S', 'a', (old-style string) 'V' (void) not handled
        #       datetime and timedelta both map to datetime (is timedelta handled?)
        _k = _DtypeKind
        _np_kinds = {'i': _k.INT, 'u': _k.UINT, 'f': _k.FLOAT, 'b': _k.BOOL,
                     'U': _k.STRING,
                     'M': _k.DATETIME, 'm': _k.DATETIME}
        
        # Categorical data for_VaexDataframe must be 23
        # if we are transfering from Vaex we must set kind to data type kind(not categorical)
        if bool_c:
            kind = 23
        else:
            kind = _np_kinds.get(dtype.kind, None)
        
        if kind is None:
            raise ValueError(f"Data type {dtype} not supported by exchange"
                                 "protocol")

        if kind not in (_k.INT, _k.UINT, _k.FLOAT, _k.BOOL, _k.CATEGORICAL):
            raise NotImplementedError(f"Data type {dtype} not handled yet")

        bitwidth = dtype.numpy.itemsize * 8
        format_str = dtype.numpy.str
        endianness = dtype.byteorder if not kind == _k.CATEGORICAL else '='
        return (kind, bitwidth, format_str, endianness)

    @property
    def describe_categorical(self) -> Dict[str, Any]:
        """
        If the dtype is categorical, there are two options:
        - There are only values in the data buffer.
        - There is a separate dictionary-style encoding for categorical values.
        Raises RuntimeError if the dtype is not categorical
        Content of returned dict:
            - "is_ordered" : bool, whether the ordering of dictionary indices is
                             semantically meaningful.
            - "is_dictionary" : bool, whether a dictionary-style mapping of
                                categorical values to other objects exists
            - "mapping" : dict, Python-level only (e.g. ``{int: str}``).
                          None if not a dictionary-style categorical.
        """
        if not self.dtype[0] == _DtypeKind.CATEGORICAL:
            raise TypeError("`describe_categorical only works on a column with "
                            "categorical dtype!")

        # ordered not impemented in Vaex    
        ordered = False
        is_dictionary = self.is_cat
        # categories.values is ndarray of length n_categories
        categories = self.labels
        mapping = {ix: val for ix, val in enumerate(categories)}
        return ordered, is_dictionary, mapping
    
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

    @property
    def null_count(self) -> int:
        """
        Number of null elements. Should always be known.
        """
        return {}

    def num_chunks(self) -> int:
        """
        Return the number of chunks the column consists of.
        """
        return 1

    def get_chunks(self, n_chunks : Optional[int] = None) -> Iterable['_PandasColumn']:
        """
        Return an iterator yielding the chunks.
        See `DataFrame.get_chunks` for details on ``n_chunks``.
        """
        return {}
    
    @property
    def metadata(self, dict_is_cat) -> Dict[str, Any]:
        """
        Store specific metadata of the column.
        """
        # Store metadata if column is categorical
        return dict_is_cat
    
    def get_data_buffer(self) -> Tuple[_VaexBuffer, Any]:  # Any is for self.dtype tuple
        """
        Return the buffer containing the data.
        """
        _k = _DtypeKind
        if self.dtype[0] in (_k.INT, _k.UINT, _k.FLOAT, _k.BOOL):
            buffer = _VaexBuffer(self._col.to_numpy())
            dtype = self.dtype
        elif self.dtype[0] == _k.CATEGORICAL:
            codes = self._col.values # uporabi nujno kode!!!
            buffer = _VaexBuffer(codes)
            bool_c = False # We want dtype of the values to set the buffer for ndarray (output wouldn't be correct if they stayed categorical)
            dtype = self._dtype_from_vaexdtype(self._col.dtype, bool_c)
        else:
            raise NotImplementedError(f"Data type {self._col.dtype} not handled yet")

        return buffer, dtype
    
    def get_mask(self) -> _VaexBuffer:
        """
        Return the buffer containing the mask values indicating missing data.
        Raises RuntimeError if null representation is not a bit or byte mask.
        """
        #null, value = self.describe_null
        #if null == 0:
        #    msg = "This column is non-nullable so does not have a mask"
        #elif null == 1:
        #    msg = "This column uses NaN as null so does not have a separate mask"
        #else:
        #    raise NotImplementedError('See self.describe_null')

        #raise RuntimeError(msg)
        return {}
    
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
        
    @property
    def metadata(self):
        is_category = {}
        labels = {}
        for i in self._df.get_names():
            is_category[i] = self._df.is_category(i)
            if self._df.is_category(i):
                labels[i] = self._df.category_labels(i)
        return {"vaex.cetagories_bool": is_category, "vaex.cetagories": labels}
        
    def num_columns(self) -> int:
        return len(self._df.columns)

    def num_rows(self) -> int:
        return len(self._df)

    def num_chunks(self) -> int:
        return 1

    def column_names(self) -> Iterable[str]:
        return self._df.get_column_names()

    def get_column(self, i: int) -> _VaexColumn:
        return _VaexColumn(self._df[:, i], self.metadata)

    def get_column_by_name(self, name: str) -> _VaexColumn:
        return _VaexColumn(self._df[name], self.metadata)

    def get_columns(self) -> Iterable[_VaexColumn]:
        return [_VaexColumn(self._df[name], self.metadata) for name in self._df.columns]

    def select_columns(self, indices: Sequence[int]) -> '_VaexDataFrame':
        if not isinstance(indices, collections.Sequence):
            raise ValueError("`indices` is not a sequence")

        return _VaexDataFrame(self._df[:, indices])

    def select_columns_by_name(self, names: Sequence[str]) -> '_VaexDataFrame':
        #if not isinstance(names, collections.Sequence):
        #    raise ValueError("`names` is not a sequence")

        #return _VaexDataFrame(self._df.xs(indices, axis='columns'))
        return {}

    def get_chunks(self, n_chunks : Optional[int] = None) -> Iterable['_VaexDataFrame']:
        """
        Return an iterator yielding the chunks.
        """
        #return (self,)  
        return {}
