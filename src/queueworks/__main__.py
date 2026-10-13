"""Support ``python -m queueworks`` as well as the installed console command."""

from queueworks.cli import main

raise SystemExit(main())
