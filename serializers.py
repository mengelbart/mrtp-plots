import pyarrow as pa
import pyarrow.feather as feather


def read_feather(file):
    table = pa.ipc.open_file(file).read_all()
    df = table.to_pandas()
    return df


def write_feather(df, file):
    table = pa.Table.from_pandas(df)
    feather.write_feather(table, file)
