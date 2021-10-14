from lxml.builder import E


def create_subfield(sub_code):
    """
    Create an empty subfield with a subfield code
    """
    new_subfield = E.subfield(code='{}'.format(sub_code))
    return new_subfield


def create_datafield(marc_key, subfield_tuple_lists=False):
    """
    marc_key: Five letter value: E.g. 245__
    The subfield_tuple_list is on the form:
    [('a', 'subfield text A'),
     ('b', 'subfield text B')]
    """
    new_datafield = E.datafield(TAG='{}'.format(marc_key[:3]),
                                ind1='{}'.format(marc_key[3].replace("_", "")),
                                ind2='{}'.format(marc_key[4].replace("_", "")))

    for subfield in subfield_tuple_lists:
        new_subfield = create_subfield(subfield[0])
        new_subfield.text = subfield[1]
        if new_subfield.text:
            new_datafield.append(new_subfield)

    return new_datafield
