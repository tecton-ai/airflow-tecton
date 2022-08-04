# Copyright 2022 Tecton, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import argparse
import sys

LICENSE_LINES = """
Copyright 2022 Tecton, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
""".strip().split(
    "\n"
)


def main(argv) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("filenames", nargs="*", help="Filenames to check")
    args = parser.parse_args(argv)

    all_good = True

    for filename in args.filenames:
        with open(filename) as file_lines:
            counter = 0
            for line in file_lines:
                if line.strip().endswith(LICENSE_LINES[counter].strip()):
                    counter += 1
                else:
                    if counter > 0:
                        break
                if counter == len(LICENSE_LINES):
                    break
            if counter != len(LICENSE_LINES):
                all_good = False
                print(f"file {filename} does not have license")

    return all_good


if __name__ == "__main__":
    res = main(sys.argv)
    if not res:
        raise Exception("missing licenses!")
