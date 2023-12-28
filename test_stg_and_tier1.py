import pytest
import numpy as np
import logging
from data_model.test_suite.helper_function import (
    extract_unittest_conf,
    transform_unittest_conf,
    define_src_trg_query,
    execute_query_cmd
)
# Set logging serverity
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

#============== Define condition and parameterized to test case ================
test_params_df = extract_unittest_conf(
    "./data_model/config_tier1/test_case_parameters.xlsx"
)
test_id, test_params = transform_unittest_conf(test_params_df)
parameterized = [define_src_trg_query(*case) for case in test_params]

# print(test_params)
# print(parameterized[1])

#====================== Parsing parameter from excel file ======================
@pytest.mark.parametrize(
    "source_query, target_query",
    parameterized,
    ids=test_id
)
def test_validate_tier1(source_query, target_query):
    source_output = execute_query_cmd(source_query)
    target_output = execute_query_cmd(target_query)
    logging.info("""
    Source query have been executed: {source_query}
    ***************************************
    Target query have been executed: {target_query}""".format(
            source_query = source_query,
            target_query = target_query
        )
    )
    # True if two arrays have the same shape and elements, False otherwise.
    # Assert True >> PASSED
    assert np.array_equal(
        np.sort(source_output.to_numpy()),
        np.sort(target_output.to_numpy())
    )