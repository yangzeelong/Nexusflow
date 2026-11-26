@echo off
rem Builds the two comparison programs with MSVC. Not part of the CMake build.
call "C:\Softwares\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat" >nul
set SRC=%~dp0
set TF=C:\Code\3rdparty\taskflow
set TBB=C:\Code\3rdparty\oneapi-tbb-win\oneapi-tbb-2021.13.0

cl /nologo /O2 /std:c++20 /EHsc /I "%TF%" /I "%TBB%\include" "%SRC%tbb_graph.cpp" ^
   /Fe:"%SRC%cmp_tbb.exe" /link /LIBPATH:"%TBB%\lib\intel64\vc14" tbb12.lib || exit /b 1
cl /nologo /O2 /std:c++20 /EHsc /I "%TF%" "%SRC%tf_pipeline.cpp" /Fe:"%SRC%cmp_tf.exe" || exit /b 1
copy /y "%TBB%\redist\intel64\vc14\tbb12.dll" "%SRC%tbb12.dll" >nul
echo build-ok
