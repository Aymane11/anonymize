from anonymize.models.rules import FakeTransform, HashTransform
import pytest
from contextlib import nullcontext as does_not_raise


@pytest.mark.parametrize(
    "algorithm,expectation",
    [
        ("md5", does_not_raise()),
        ("invalid_algo", pytest.raises(ValueError, match=r"algorithm must be one of .+")),
    ],
)
def test_hash_transform_algorithm(algorithm, expectation):
    with expectation:
        HashTransform(column="column_name", method="hash", algorithm=algorithm, salt="salt_value")


@pytest.mark.parametrize(
    "fake_method,expectation",
    [
        ("email", does_not_raise()),
        ("number", pytest.raises(ValueError, match=r"faker_type must be one of .+")),
    ],
)
def test_fake_method(fake_method, expectation):
    with expectation:
        FakeTransform(column="column_name", method="fake", faker_type=fake_method)
