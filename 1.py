from dataclasses import dataclass

@dataclass
class TimeConfig:
    hours: int = 0
    minutes: int = 0
    seconds: int = 0

    def total_seconds(self) -> int:
        """Calculate total time in seconds."""
        return self.hours * 3600 + self.minutes * 60 + self.seconds

    def to_x_message_ttl(self) -> int:
        """Convert to x-message-ttl format (in milliseconds)."""
        return self.total_seconds() * 1000

    def to_expiration(self) -> str:
        """Convert to expiration format (as a string in milliseconds)."""
        return str(self.to_x_message_ttl())

    @classmethod
    def from_string(cls, time_str: str):
        """Create a TimeConfig from a string in the format 'HH:MM:SS'."""
        parts = time_str.split(':')
        if len(parts) != 3:
            raise ValueError("Time string must be in 'HH:MM:SS' format.")
        
        hours, minutes, seconds = map(int, parts)
        return cls(hours, minutes, seconds)

    def __str__(self) -> str:
        """Return a string representation of the time in 'HH:MM:SS' format."""
        return f"{self.hours:02}:{self.minutes:02}:{self.seconds:02}"

# Usage examples
time_config = TimeConfig(hours=1, minutes=30, seconds=45)
print(f"Total seconds: {time_config.total_seconds()}")  # Output: Total seconds: 5445
print(f"x-message-ttl (ms): {time_config.to_x_message_ttl()}")  # Output: x-message-ttl (ms): 5445000
print(f"Expiration (str): {time_config.to_expiration()}")  # Output: Expiration (str): 5445000

# Create from string
time_from_str = TimeConfig.from_string("02:15:30")
print(f"Time from string: {time_from_str}")  # Output: Time from string: 02:15:30
