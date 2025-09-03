import subprocess
from pathlib import Path
from setuptools import setup
from setuptools.command.build_py import build_py


import shutil

class BuildSvelte(build_py):
    """Custom build command to build the Svelte frontend."""

    def run(self):
        svelte_dir = Path(__file__).parent / "svelte"
        static_dir = Path(__file__).parent / "src" / "lijnding_ui" / "static"

        if static_dir.exists():
            shutil.rmtree(static_dir)

        if not (svelte_dir / "node_modules").exists():
            print("Installing Svelte dependencies...")
            subprocess.run(["npm", "install"], cwd=svelte_dir, check=True)

        print("Building Svelte frontend...")
        subprocess.run(["npm", "run", "build"], cwd=svelte_dir, check=True)

        shutil.copytree(svelte_dir / "build", static_dir)

        super().run()


setup(
    cmdclass={
        "build_py": BuildSvelte,
    },
)
