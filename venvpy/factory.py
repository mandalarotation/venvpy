import json
import logging
import os
import shutil
import socket
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from functools import reduce, wraps

import requests

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


class Socket:
    def __init__(self, port_range: tuple):
        self.port, self.max_port = port_range

    def next_free_port(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        while self.port <= self.max_port:
            try:
                sock.bind(("", self.port))
                sock.close()
                return self.port
            except OSError:
                self.port += 1
        raise IOError("no free ports")


class PythonFactory(Socket):
    def __init__(
        self, port_range: tuple = (1024, 65535), processes: int = 2, threads: int = 2
    ):

        self.factories: list = []
        self.processes = processes
        self.threads = threads
        Socket.__init__(self, port_range)

    def factory_fund(self, processes=1, threads=1) -> None:
        @dataclass(frozen=True)
        class PythonVenv:
            python_version: str
            imports: list = field(default_factory=list)
            libs: list = field(default_factory=list)
            after_install_cmd: str = ""
            venvs_dir: str = os.path.join("/tmp", str(uuid.uuid1()))
            verbose: bool = False
            envs: list = field(default_factory=list)
            port: int = self.next_free_port()
            processes: int = self.processes
            threads: int = self.threads

            def __post_init__(
                self,
            ) -> None:

                if not os.path.exists(self.venvs_dir):
                    os.makedirs(self.venvs_dir)

                self.envs.append(
                    {
                        **os.environ,
                        "PYENV_ROOT": os.path.join(self.venvs_dir, ".pyenv"),
                        "PATH": os.path.join(
                            self.venvs_dir,
                            ".pyenv/versions",
                            self.python_version,
                            "bin:",
                        )
                        + os.path.join(self.venvs_dir, ".pyenv/bin:")
                        + str({**os.environ}["PATH"]),
                        "PYTHONPATH": os.path.join(
                            self.venvs_dir,
                            ".pyenv/versions",
                            self.python_version,
                            "lib/python" + self.python_version[:-2],
                            "site-packages:",
                        ),
                        **(self.envs[0] if self.envs else {}),
                    }
                )
                self.__install_venv()
                if self.libs:
                    self.__install_libs()

                self.__server()

            def __install_libs(self) -> None:
                query_libs = " ".join(self.libs)
                self.__run_bash_cmd(
                    f"pip install {query_libs}",
                )

            def __install_venv(self) -> None:

                self.__run_bash_cmd(
                    "apt update && apt install -y libsm6 libxext6 libxrender-dev -y",
                )

                if self.after_install_cmd:
                    self.__run_bash_cmd(
                        self.after_install_cmd,
                    )

                self.__run_bash_cmd(
                    f"git clone https://github.com/pyenv/pyenv.git {self.envs[0]['PYENV_ROOT']}",
                )
                self.__run_bash_cmd(
                    f"pyenv install {self.python_version}",
                )

                self.__run_bash_cmd(
                    f"""pip install \
                            flask \
                            waitress \
                            uwsgi \
                            cloudpickle""",
                )

            def reinstall(self) -> None:
                self.__run_bash_cmd(
                    f"pyenv uninstall {self.python_version}",
                )
                shutil.rmtree(self.venvs_dir)
                self.__install_venv()
                self.__install_libs()
                self.__server()

            def reactivate(self) -> None:
                self.__server()

            def __run_bash_cmd(
                self,
                cmd: str,
            ) -> None:
                """Run a bash command controling its log rate.
                :param cmd: bash command that will be executed
                :param verbose: if True verbose is enabled, defaults to False
                :param verbose_waiting_time: seconds between shown logs, defaults to 0
                :param verbose_src: verbose source, could be 'stderr' or 'stdout', defaults to 'stderr'.
                """
                verbose_waiting_time = 0
                verbose_src = "stdout"

                if verbose_src not in [
                    "stderr",
                    "stdout",
                ]:
                    raise ValueError(
                        "verbose_src must be equal to 'stderr' or 'stdout'"
                    )
                logger.info(
                    "\tStarting a process! Verbose: %s. Verbose time: %s. \
                    Command thats is being excecuted:\n\t\t%s",
                    self.verbose,
                    verbose_waiting_time,
                    " ".join(cmd.split()),
                )
                with subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    shell=True,  # nosec
                    universal_newlines=True,
                    env=reduce(lambda env1, env2: {**env1, **env2}, self.envs),
                ) as process:
                    try:
                        if self.verbose:
                            pipe = (
                                process.stdout
                                if verbose_src == "stdout"
                                else process.stderr
                            )

                            start_time = time.time()
                            while True:
                                p_status = process.poll()
                                if p_status is not None:
                                    break

                                if pipe:
                                    line = pipe.readline() if pipe else ""
                                if time.time() - start_time >= verbose_waiting_time:
                                    logger.info("\t\t%s", line.strip())
                                    start_time = time.time()
                                    continue
                        else:
                            # If verbose is disabled, wait until process finishes
                            p_status = process.wait()

                        logger.info(
                            "\tProcess has just finished! Exit code: %s", p_status
                        )
                    except Exception:  # pylint: disable=broad-except
                        logger.error("\tKilling process!")
                        process.kill()

                if p_status != 0:
                    logger.warning("\tSomething went wrong!")

            def set_env(self, func):
                @wraps(func)
                def wrapper(*args, **kwargs):

                    msg = {
                        "fn": func.__name__.split("_")[0],
                        "args": args,
                        "kwargs": kwargs,
                    }

                    payload = json.dumps(msg)

                    url = f"http://0.0.0.0:{self.port}/bind"

                    headers = {"Content-Type": "application/json"}

                    response = requests.request(
                        "POST", url, headers=headers, data=payload
                    )

                    result = response.json()["result"]

                    return result

                return wrapper

            def __server(self) -> None:

                code_lines = [
                    "from flask import Flask, request, jsonify",
                    "app = Flask(__name__)",
                    "@app.route('/bind',methods=['GET','POST'])",
                    "def index():",
                    "    context = request.json",
                    "    fn = globals()[context['fn']]",
                    "    result = fn(*context['args'],**context['kwargs'])",
                    "    response = jsonify({'result': result})",
                    "    return response",
                    "application = app",
                ]

                for _impor in list(self.imports):
                    code_lines = [_impor] + code_lines

                code_text = "\n".join(code_lines)

                entrypoint = os.path.join(self.venvs_dir, "app.py")

                with open(entrypoint, "w") as file:
                    file.write(code_text)

                cmd = f"""nohup uwsgi --wsgi-file {entrypoint} \
                            --http 0.0.0.0:{self.port} --processes {self.processes} --threads {self.threads} --master \
                            --pidfile {os.path.join(self.venvs_dir, 'myapp.pid')} & echo OK"""

                self.__run_bash_cmd(cmd)

            def close(self):

                cmd = f"uwsgi --stop {os.path.join(self.venvs_dir, 'myapp.pid')}"

                self.__run_bash_cmd(cmd)

                del self

        self.factories.append(PythonVenv)
