from pathlib import Path
import logging
import os
import time

import xmltodict

from arteria.models.state import State


log = logging.getLogger(__name__)


def list_runfolders(path, filter_key=lambda r: True):
    return []


class Runfolder():
    def __init__(self, path, grace_minutes=0):
        self.path = Path(path)

        assert self.path.is_dir()
        try:
            run_parameter_file = next(
                path
                for path in [
                    self.path / "RunParameters.xml",
                    self.path / "runParameters.xml",
                ]
                if path.exists()
            )
            self.run_parameters = xmltodict.parse(run_parameter_file.read_text())["RunParameters"]
        except StopIteration:
            raise AssertionError("File [Rr]unParameters.xml not found in runfolder {path}")

        marker_file = Instrument(self.run_parameters).completed_marker_file
        assert (
            marker_file.exists()
            and time.time() - os.path.getmtime(marker_file) > grace_minutes * 60
        )

        (self.path / ".arteria").mkdir(exist_ok=True)
        self._state_file = (self.path / ".arteria/state")
        if not self._state_file.exists():
            self._state_file.write_text("ready")

    @property
    def state(self):
        return State(self._state_file.read_text().strip())

    @state.setter
    def state(self, new_state):
        assert new_state in State
        self._state_file.write_text(new_state.value)

    @property
    def metadata(self):
        if not self.run_parameters:
            log.warning(f"No metadata found for runfolder {self.path}")

        metadata = {}

        try:
            metadata["reagent_kit_barcode"] = \
                self.run_parameters["ReagentKitBarcode"]
        except KeyError:
            log.debug("Reagent kit barcode not found")

        try:
            metadata["library_tube_barcode"] = \
                self.run_parameters["RfidsInfo"]["LibraryTubeSerialBarcode"]
        except KeyError:
            try:
                metadata["library_tube_barcode"] = \
                    next(
                        consumable["SerialNumber"]
                        for consumable in self.run_parameters["ConsumableInfo"]["ConsumableInfo"]
                        if consumable["Type"] == "SampleTube"
                    )
            except (KeyError, StopIteration):
                log.debug("Library tube barcode not found")

        return metadata


class Instrument:
    def __init__(self, run_params_file):
        pass
