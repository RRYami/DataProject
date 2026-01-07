"""
Pydantic models for ticker/indice operations.

These models define the structure for request and response data.
"""

from pydantic import BaseModel, Field


class IndiceCreate(BaseModel):
    """Model for creating a new indice."""

    indice: str = Field(
        ..., description="Ticker symbol of the indice", min_length=1
    )
    name: str = Field(..., description="Name of the indice", min_length=1)
    market: str = Field(..., description="Market where the indice is listed")
    locale: str = Field(..., description="Locale of the indice")
    active: bool = Field(..., description="Whether the indice is active")
    source_feed: str = Field(..., description="Source feed of the indice")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "indice": "SPX",
                    "name": "S&P 500 Index",
                    "market": "indices",
                    "locale": "us",
                    "active": True,
                    "source_feed": "polygon",
                }
            ]
        }
    }


class IndiceResponse(BaseModel):
    """Response model for indice operations."""

    message: str = Field(..., description="Success or error message")
