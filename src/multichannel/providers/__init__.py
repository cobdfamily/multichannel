"""Outbound provider dispatch API."""

from multichannel.providers.dispatch import DispatchError, dispatch_message

__all__ = ["DispatchError", "dispatch_message"]
"""Provider adapter exports."""

from multichannel.providers.dispatch import dispatch_message

__all__ = ["dispatch_message"]
