# post-install.cmake — create ~/.pegasus/healer.env on first install
#
# Called via install(SCRIPT ...) from the root CMakeLists.txt.
# SRC_DIR is injected by the caller via install(CODE "set(SRC_DIR ...)").

find_program(_healer_python NAMES python3 python)

if(NOT _healer_python)
    message(WARNING "[healer] Python not found — skipping ~/.pegasus/healer.env setup")
    return()
endif()

set(_example "${SRC_DIR}/packages/pegasus-healer/src/Pegasus/healer/healer.env.example")

execute_process(
    COMMAND "${_healer_python}" -c "
import shutil, stat, os
from pathlib import Path

target  = Path.home() / '.pegasus' / 'healer.env'
example = Path(r'${_example}')

if target.exists():
    print('[healer] ~/.pegasus/healer.env already exists — not overwritten')
elif not example.exists():
    print('[healer] WARNING: healer.env.example not found at ' + str(example))
else:
    target.parent.mkdir(exist_ok=True)
    shutil.copy(str(example), str(target))
    target.chmod(stat.S_IRUSR | stat.S_IWUSR)
    print('[healer] Created ~/.pegasus/healer.env')
    print('[healer] Edit it and set LLM_MODEL + LLM_API_KEY before use.')
"
    RESULT_VARIABLE _result
)

if(NOT _result EQUAL 0)
    message(WARNING "[healer] post-install script failed (exit ${_result})")
endif()
