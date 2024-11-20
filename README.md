# Sample Application for Async Transaction Manager

This simple sample application shows what happens if an error occurs during the commit, and the
application after that calls `rollbackAsync`. The latter fails, as the transaction has already
finished.
