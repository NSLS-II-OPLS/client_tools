from databroker.queries import ScanID
from tiled.client import from_uri
from tiled.structures.core import StructureFamily

import datetime
import h5py
import sys
import warnings


def _serialize_special_types(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    elif obj is None:
        return "None"
    else:
        return obj


def _iter_list(attr_dict, meta_list, iter_key):
    list_iter_key = iter_key
    for i in range(len(meta_list)):
        node_key = list_iter_key + str(i)
        if isinstance(meta_list[i], dict):
            attr_dict = _iter_dictionary(attr_dict, meta_list[i], node_key + ".")  # noqa: E501
        elif isinstance(meta_list[i], list):
            attr_dict = _iter_list(attr_dict, meta_list[i], node_key + ".")
        else:
            attr_dict[node_key] = _serialize_special_types(meta_list[i])
    return attr_dict


def _iter_dictionary(attr_dict, metadata, iter_key=""):
    next_iter_key = iter_key
    for key, value in metadata.items():
        if isinstance(value, dict):
            attr_dict = _iter_dictionary(attr_dict, value, next_iter_key + key + ".")  # noqa: E501
        elif isinstance(value, list):
            attr_dict = _iter_list(attr_dict, value, next_iter_key + key + ".")
        else:
            node_key = next_iter_key + key
            attr_dict[node_key] = _serialize_special_types(value)

    return attr_dict


def metadata_to_attribute(metadata):
    """
    This method receives some metadata in the form of a nested dictionary and
    converts it into a flattened version of itself to make it compatible with
    HDF5 standard for an attribute object.

    Parameters
    ----------
    metadata : dict
        Nested metadata.

    Returns
    -------
    attr_dict : dict
        Flattened metadata.

    """

    attr_dict = {}
    attr_dict = _iter_dictionary(attr_dict, metadata)

    return attr_dict


def walk(node, pre=None):
    """
    Yield (key_path, value) where each value is an ArrayAdapter.

    As a succinct illustration (does not literally run):

    >>> list(walk{"a": {"b": 1, "c": {"d": 2}}})
    [
        (("a", "b"), 1),
        (("a", "c", "d"), 2),
    ]
    """
    pre = pre[:] if pre else []
    if node.item["attributes"]["structure_family"] != StructureFamily.array:
        for key, value in node.items():
            for d in walk(value, pre + [key]):
                yield d
    else:
        yield (pre, node)


def export(node, filepath):
    root_node = node
    with h5py.File(filepath, mode="w") as file:
        file.attrs.update(metadata_to_attribute(node.metadata))
        for key_path, array_client in walk(node):
            group = file
            node = root_node
            for key in key_path[:-1]:
                node = node[key]
                if key in group:
                    group = group[key]
                else:
                    group = group.create_group(key)
                    group.attrs.update(metadata_to_attribute(node.metadata))
            try:
                data = array_client.read()
            except:
                warnings.warn(f"Could not read path: {key_path}")
            if data.dtype.kind == "U":
                data = data.astype("S")
            dataset = group.create_dataset(key_path[-1], data=data.tolist())
            for k, v in metadata_to_attribute(array_client.metadata).items():
                dataset.attrs.create(k, v)


if __name__ == "__main__":
    client = from_uri("https://tiled.nsls2.bnl.gov/api/v1/metadata/opls/raw")
    # print(f'{client.context.whoami()}')
    scan_ids = sys.argv[1:]

    results = client.search(ScanID(*scan_ids))
    export(results, "test_hdf5.h5")
    print("Done")
