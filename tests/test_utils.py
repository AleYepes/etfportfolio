from etfportfolio.core.utils import canonical_bytes, content_address, decompress_payload


def test_canonical_bytes_dict_order_invariance():
    obj1 = {"b": 2, "a": 1, "nested": {"z": 26, "y": 25}}
    obj2 = {"nested": {"y": 25, "z": 26}, "a": 1, "b": 2}

    assert canonical_bytes(obj1) == canonical_bytes(obj2)


def test_canonical_bytes_list_order_invariance():
    obj1 = {"items": [3, 1, 2], "tags": ["beta", "alpha"]}
    obj2 = {"tags": ["alpha", "beta"], "items": [1, 2, 3]}

    assert canonical_bytes(obj1) == canonical_bytes(obj2)


def test_canonical_bytes_mixed_types():
    list1 = [None, True, 10, "str", {"k": "v"}, [1, 2]]
    list2 = [[1, 2], "str", {"k": "v"}, 10, True, None]

    assert canonical_bytes(list1) == canonical_bytes(list2)


def test_content_address_and_decompress():
    data = {"name": "test_fund", "holdings": [{"id": 101, "weight": 0.5}, {"id": 102, "weight": 0.5}]}
    digest, compressed = content_address(data)

    assert isinstance(digest, int)
    assert digest >= 0  # Unsigned 64-bit int
    assert isinstance(compressed, bytes)

    decompressed = decompress_payload(compressed)
    # Check that contents match (lists might be canonicalized/sorted)
    assert decompressed["name"] == "test_fund"
    assert len(decompressed["holdings"]) == 2
