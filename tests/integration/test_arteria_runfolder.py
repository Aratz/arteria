from pathlib import Path
import tempfile

import pytest

from arteria import __version__
from arteria.services.arteria_runfolder import get_app


@pytest.fixture()
def config():
    with tempfile.TemporaryDirectory() as monitored_dir:
        runfolder = Path(monitored_dir) / "200624_A00834_0183_BHMTFYDRXX"
        runfolder.mkdir()

        yield {
            "monitored_directories": [monitored_dir],
        }


@pytest.fixture()
async def client(config, aiohttp_client):
    app = get_app(config)
    return await aiohttp_client(app)


async def test_version(client):
    async with client.request("GET", "/version") as resp:
        assert resp.status == 200
        content = await resp.json()

    assert content == {"version": __version__}


async def test_post_runfolders_path(client, config):
    async with client.request(
            "POST",
            "/runfolders/path/200624_A00834_0183_BHMTFYDRXX",
            data={"state": "STARTED"}) as resp:
        assert resp.status == 200

        state = Path(config["monitored_directories"][0]) / ".arteria/state"

        with open(state) as state_file:
            assert state_file.read() == "started"


async def test_post_runfolders_path_invalid_state(client):
    async with client.request(
            "POST",
            "/runfolders/path/200624_A00834_0183_BHMTFYDRXX",
            data={"state": "INVALID"}) as resp:

        assert resp.status == 400
        assert resp.text == "The state 'INVALID' is not valid"


async def test_post_runfolders_path_missing_runfolder(client):
    async with client.request(
            "POST",
            "/runfolders/path/200624_A00834_0183_FAKE_RUNFOLDER",
            data={"state": "STARTED"}) as resp:

        assert resp.status == 404
        assert resp.text == "Runfolder '200624_A00834_0183_FAKE_RUNFOLDER' does not exist"
