from fastapi import Request, HTTPException, status
from typing import Optional, Set, Dict
import re
from datetime import datetime
import mysql.connector
from .database import DatabaseConnection
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

class DataValidator:
    """Data validation service for the RPA Land Use API."""
    
    def __init__(self):
        self._valid_scenarios: Set[str] = set()
        self._valid_fips: Set[str] = set()
        self._valid_land_uses: Set[str] = set()
        self._valid_years: Set[int] = set()
        self._last_refresh: datetime = datetime.min
        self._refresh_interval: int = 3600  # Refresh validation data every hour

    async def initialize(self):
        """Initialize the validator by loading valid values from the database."""
        self._refresh_validation_data()

    def _refresh_validation_data(self) -> None:
        """Refresh validation data from the database if needed."""
        now = datetime.now()
        if (now - self._last_refresh).total_seconds() < self._refresh_interval:
            return

        conn = None
        try:
            conn = DatabaseConnection.get_connection()
            cursor = conn.cursor(dictionary=True)

            # Get valid scenarios
            cursor.execute("SELECT scenario_name FROM scenarios")
            self._valid_scenarios = {row['scenario_name'] for row in cursor.fetchall()}

            # Get valid FIPS codes
            cursor.execute("SELECT DISTINCT fips_code FROM counties")
            self._valid_fips = {row['fips_code'] for row in cursor.fetchall()}

            # Get valid land use types
            cursor.execute("""
                SELECT DISTINCT from_land_use as land_use FROM land_use_transitions
                UNION
                SELECT DISTINCT to_land_use as land_use FROM land_use_transitions
            """)
            self._valid_land_uses = {row['land_use'] for row in cursor.fetchall()}

            # Get valid years from time_steps
            cursor.execute("SELECT start_year, end_year FROM time_steps")
            self._valid_years = set()
            for row in cursor.fetchall():
                self._valid_years.update(range(row['start_year'], row['end_year'] + 1))

            self._last_refresh = now

        except mysql.connector.Error as err:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to load validation data: {str(err)}"
            )
        finally:
            if cursor:
                cursor.close()
            if conn:
                DatabaseConnection.close_connection(conn)

    def validate_scenario(self, scenario: Optional[str]) -> None:
        """Validate scenario name."""
        if scenario and scenario not in self._valid_scenarios:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid scenario: {scenario}. Valid scenarios are: {sorted(self._valid_scenarios)}"
            )

    def validate_year(self, year: Optional[int]) -> None:
        """Validate year."""
        if year and year not in self._valid_years:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid year: {year}. Year must be between {min(self._valid_years)} and {max(self._valid_years)}"
            )

    def validate_fips(self, fips: Optional[str]) -> None:
        """Validate FIPS code."""
        if fips:
            if not re.match(r'^\d{5}$', fips):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="FIPS code must be exactly 5 digits"
                )
            if fips not in self._valid_fips:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid FIPS code: {fips}"
                )

    def validate_land_use(self, land_use: Optional[str], param_name: str) -> None:
        """Validate land use type."""
        if land_use and land_use not in self._valid_land_uses:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid {param_name}: {land_use}. Valid land use types are: {sorted(self._valid_land_uses)}"
            )

# Global validator instance
validator = DataValidator()

class ValidationMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        """Middleware to validate request parameters."""
        try:
            # Refresh validation data if needed
            validator._refresh_validation_data()

            # Get query parameters
            params = dict(request.query_params)
            
            # Validate parameters based on the endpoint
            path = request.url.path
            
            if path == "/transitions":
                validator.validate_scenario(params.get("scenario"))
                if "year" in params:
                    validator.validate_year(int(params["year"]))
                validator.validate_fips(params.get("fips"))
                validator.validate_land_use(params.get("from_use"), "from_use")
                validator.validate_land_use(params.get("to_use"), "to_use")
            
            # Continue with the request
            response = await call_next(request)
            return response
        except HTTPException as exc:
            return Response(
                content=str({"detail": exc.detail}),
                status_code=exc.status_code,
                media_type="application/json"
            ) 