# Release handling

Once target functionality has been completed and verified...

1. The `VERSION` variable in `setup.py` is changed to the target new semantic
   version.
2. The change is committed and pushed to `master`.
3. `python setup.py upload` is run, which makes the new package version
   available on [PyPi](https://pypi.org/project/rabbitmq-client/).
