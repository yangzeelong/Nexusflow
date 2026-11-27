@echo off
rem Builds the comparison programs with MSVC. Not part of the CMake build.
rem Keep .obj output next to the sources (this directory is gitignored for *.obj).
cd /d "%~dp0"
call "C:\Softwares\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat" >nul
set SRC=%~dp0
set TF=C:\Code\3rdparty\taskflow
set TBB=C:\Code\3rdparty\oneapi-tbb-win\oneapi-tbb-2021.13.0

cl /nologo /O2 /DNDEBUG /std:c++20 /EHsc /I "%TF%" /I "%TBB%\include" "%SRC%tbb_graph.cpp" ^
   /Fe:"%SRC%cmp_tbb.exe" /link /LIBPATH:"%TBB%\lib\intel64\vc14" tbb12.lib || exit /b 1
cl /nologo /O2 /DNDEBUG /std:c++20 /EHsc /I "%TF%" "%SRC%tf_pipeline.cpp" /Fe:"%SRC%cmp_tf.exe" || exit /b 1
cl /nologo /O2 /DNDEBUG /std:c++20 /EHsc /I "%TBB%\include" "%SRC%tbb_pipeline.cpp" ^
   /Fe:"%SRC%cmp_tbbpipe.exe" /link /LIBPATH:"%TBB%\lib\intel64\vc14" tbb12.lib || exit /b 1
cl /nologo /O2 /DNDEBUG /std:c++20 /EHsc "%SRC%raw_pipeline.cpp" /Fe:"%SRC%cmp_raw.exe" || exit /b 1
rem ai-pipe: public headers only (src/api); the library is the prebuilt static lib
rem (built with the DLL runtime, so /MD is required to match its RuntimeLibrary).
cl /nologo /O2 /DNDEBUG /std:c++20 /EHsc /MD /I "C:\Code\ai-pipe-ref\src\api" "%SRC%ai_pipeline.cpp" ^
   /Fe:"%SRC%cmp_ai.exe" /link /LIBPATH:"C:\Code\ai-pipe-ref\build-msvc\AMD64\lib\Release" ai_pipe.lib || exit /b 1
copy /y "%TBB%\redist\intel64\vc14\tbb12.dll" "%SRC%tbb12.dll" >nul
echo build-ok
