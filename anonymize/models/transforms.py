from typing import Literal, Union
from pydantic import BaseModel, Field, validator


class HashTransform(BaseModel):
    method: Literal["hash"]
    algorithm: str
    salt: str

    @validator("algorithm")
    def validate_algorithm(cls, v):
        import hashlib

        if v not in hashlib.algorithms_available:
            raise ValueError(f"algorithm must be one of {hashlib.algorithms_available}")
        return v


class FakeTransform(BaseModel):
    method: Literal["fake"]
    faker_type: str

    @validator("faker_type")
    def validate_type(cls, v):
        AVAILABLE_TYPES = {"email", "firstname"}
        if v not in AVAILABLE_TYPES:
            raise ValueError(f"faker_type must be one of {AVAILABLE_TYPES}")
        return v


class MaskRightTransform(BaseModel):
    method: Literal["mask_right"]
    n_chars: int = Field(..., gt=1)
    mask_char: str = Field(min_length=1, max_length=1)

    class Config:
        coerce_numbers_to_str = True


class MaskLeftTransform(BaseModel):
    method: Literal["mask_left"]
    n_chars: int = Field(..., gt=1)
    mask_char: str = Field(min_length=1, max_length=1)

    class Config:
        coerce_numbers_to_str = True


Transform = Union[HashTransform, FakeTransform, MaskRightTransform, MaskLeftTransform]
