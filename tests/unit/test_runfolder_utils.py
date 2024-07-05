from pathlib import Path
import shutil
import tempfile

import pytest
import unittest.mock as mock

from arteria.models.runfolder_utils import list_runfolders, Runfolder, Instrument
from arteria.models.state import State
from arteria.models.config import Config


@pytest.fixture()
def monitored_directory():
    with tempfile.TemporaryDirectory() as monitored_dir:
        for i in range(3):
            runfolder_path = Path(monitored_dir) / f"runfolder{i}"
            runfolder_path.mkdir()
            (runfolder_path / "CopyComplete.txt").touch()

            if i == 0:
                (runfolder_path / ".arteria").mkdir()
                (runfolder_path / ".arteria/state").write_text(State.STARTED.name)

        (Path(monitored_dir) / "regular_folder").mkdir()

        yield monitored_directory


@pytest.fixture()
def runfolder(request):
    with mock.patch("arteria.models.runfolder_utils.Instrument") as instrument:
        instrument.completed_marker_file = "CopyComplete.txt"

        with tempfile.TemporaryDirectory(suffix="RUNFOLDER") as runfolder_path:
            runfolder_path = Path(runfolder_path)

            (runfolder_path / "CopyComplete.txt").touch()

            (runfolder_path / ".arteria").mkdir()
            (runfolder_path / ".arteria/state").write_text(State.STARTED.value)

            if hasattr(request, "param"):
                run_parameters_file = request.param
            else:
                run_parameters_file = "RunParameters_NSXp.xml"
            shutil.copyfile(
                f"tests/resources/{run_parameters_file}",
                Path(runfolder_path) / "RunParameters.xml",
            )

            yield Runfolder(runfolder_path)


def test_list_runfolders(monitored_directory):
    runfolders = list_runfolders(monitored_directory)

    assert len(runfolders) == 3
    assert all(
        runfolder.path == f"{monitored_directory}/runfolder{i}"
        for i, runfolder in enumerate(sorted(runfolders, key=lambda r: r.path))
    )


def test_list_runfolders_filtered(monitored_directory):
    runfolder = list_runfolders(
        monitored_directory,
        filter_key=lambda r: r.state == State.STARTED
    )

    assert len(runfolder) == 1
    assert runfolder[0].path == f"{monitored_directory}/runfolder0"
    assert runfolder[0].state == State.STARTED


class TestRunfolder():
    def test_init_regular_folder(self):
        with pytest.raises(AssertionError):
            with tempfile.TemporaryDirectory() as regular_folder:
                Runfolder(regular_folder)

    def test_init_young_runfolder(self, runfolder):
        Config.new({
            "completed_marker_grace_minutes": 60,
        })
        with pytest.raises(AssertionError):
            Runfolder(runfolder.path)

        del Config._instance

    def test_get_state(self, runfolder):
        assert runfolder.state == State.STARTED

    def test_set_state(self, runfolder):
        runfolder.state = State.DONE
        assert runfolder.state == State.DONE

    def get_path(self, runfolder):
        assert runfolder.path.endswith("RUNFOLDER")

    @pytest.mark.parametrize(
        "runfolder,metadata",
        [
            ("RunParameters_MiSeq.xml", {"reagent_kit_barcode": "MS6728155-600V3"}),
            ("RunParameters_NS6000.xml", {"library_tube_barcode": "NV0217945-LIB"}),
            ("RunParameters_NSXp.xml", {"library_tube_barcode": "LC1025031-LC1"}),
        ],
        indirect=["runfolder"],
    )
    def test_get_metadata(self, runfolder, metadata):
        assert runfolder.metadata == metadata


class TestInstrument():
    @pytest.mark.parametrize(
        "runparameter_file,marker_file",
        [
            ("tests/resources/RunParameters_MiSeq.xml", "RTAComplete.txt"),
            ("tests/resources/RunParameters_NS6000.xml", "CopyComplete.txt"),
            ("tests/resources/RunParameters_NSXp.xml", "CopyComplete.txt"),
        ]
    )
    def test_get_marker_file(self, runparameter_file, marker_file):
        instrument = Instrument(runparameter_file)
        assert instrument.completed_marker_file == marker_file
