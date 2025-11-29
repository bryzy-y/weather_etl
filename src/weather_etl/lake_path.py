from pathlib import PurePosixPath
from typing import Self


class LakePath:
    """A class for composing S3 paths with support for both URI and key formats.

    Examples:
        >>> path = LakePath("my-bucket", "data", "weather")
        >>> path.uri
        's3://my-bucket/data/weather'
        >>> path.key
        'data/weather'
        >>> path.bucket
        'my-bucket'

        >>> # Compose paths using the / operator
        >>> base = LakePath("my-bucket", "data")
        >>> full_path = base / "weather" / "2024" / "01"
        >>> full_path.uri
        's3://my-bucket/data/weather/2024/01'
    """

    def __init__(self, bucket: str, *parts: str):
        """Initialize a LakePath.

        Args:
            bucket: The S3 bucket name
            *parts: Path components to append after the bucket
        """
        self.bucket = bucket
        self._path = PurePosixPath(*parts) if parts else PurePosixPath()

    @property
    def key(self) -> str:
        """Return the S3 key (path without bucket and s3:// prefix)."""
        return str(self._path)

    @property
    def uri(self) -> str:
        """Return the fully qualified S3 URI (s3://bucket/key)."""
        return f"s3://{self.bucket}/{self.key}"

    def __truediv__(self, other: str | PurePosixPath) -> Self:
        """Compose paths using the / operator.

        Args:
            other: Path component to append

        Returns:
            A new LakePath with the appended component
        """
        new_path = self.__class__(self.bucket)
        new_path._path = self._path / other
        return new_path

    def __str__(self) -> str:
        """Return the URI representation."""
        return self.uri

    def __repr__(self) -> str:
        """Return a repr showing the bucket and key."""
        return f"LakePath(bucket='{self.bucket}', key='{self.key}')"

    def joinpath(self, *parts: str) -> Self:
        """Join multiple path components.

        Args:
            *parts: Path components to append

        Returns:
            A new LakePath with the appended components
        """
        new_path = self.__class__(self.bucket)
        new_path._path = self._path.joinpath(*parts)
        return new_path

    @classmethod
    def from_uri(cls, uri: str) -> Self:
        """Create a LakePath from an S3 URI.

        Args:
            uri: S3 URI in the format s3://bucket/key

        Returns:
            A new LakePath instance

        Raises:
            ValueError: If the URI is not a valid S3 URI
        """
        if not uri.startswith("s3://"):
            raise ValueError(f"Invalid S3 URI: {uri}")

        parts = uri[5:].split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""

        if key:
            return cls(bucket, key)
        return cls(bucket)

    @property
    def parent(self) -> Self:
        """Return the parent path."""
        new_path = self.__class__(self.bucket)
        new_path._path = self._path.parent
        return new_path

    @property
    def name(self) -> str:
        """Return the final component of the path."""
        return self._path.name

    @property
    def suffix(self) -> str:
        """Return the file extension."""
        return self._path.suffix

    def with_suffix(self, suffix: str) -> Self:
        """Return a new path with a different suffix.

        Args:
            suffix: The new suffix (e.g., '.json', '.parquet')

        Returns:
            A new LakePath with the replaced suffix
        """
        new_path = self.__class__(self.bucket)
        new_path._path = self._path.with_suffix(suffix)
        return new_path


raw_path = LakePath("dagster-weather-etl", "raw")
staged_path = LakePath("dagster-weather-etl", "staged")
