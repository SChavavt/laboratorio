from order_identifiers import identifier_resolver


def test_exact_ids_win_and_only_unique_legacy_aliases_resolve():
    resolve = identifier_resolver(['001', '01', '00012345', '02102026-001'])
    assert resolve('001') == '001'
    assert resolve('1') == '1'
    assert resolve('12345') == '00012345'
    assert resolve('02102026-001') == '02102026-001'
    assert resolve('2102026-001') == '2102026-001'
    assert identifier_resolver(['001', '1'])('1') == '1'
