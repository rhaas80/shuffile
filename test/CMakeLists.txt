###############
# Build tests
###############

################
# Add tests to ctest
################

ADD_EXECUTABLE(shuffile_test test1.c)
TARGET_LINK_LIBRARIES(shuffile_test ${SHUFFILE_EXTERNAL_LIBS} shuffile)
ADD_TEST(NAME shuffile_test COMMAND ${MPIEXEC} ${MPIEXEC_NUMPROC_FLAG} 6 -N 3 -m block ./shuffile_test)
#ADD_TEST(NAME shuffile_test COMMAND ${MPIEXEC} ${MPIEXEC_NUMPROC_FLAG} 4 ./shuffile_test)

ADD_EXECUTABLE(test_config test_config.c)
TARGET_LINK_LIBRARIES(test_config ${SHUFFILE_EXTERNAL_LIBS} shuffile)
ADD_TEST(NAME test_config COMMAND ${MPIEXEC} ${MPIEXEC_NUMPROC_FLAG} 1 ./test_config)

####################
# make a verbose "test" target named "check"
####################

ADD_CUSTOM_TARGET(check COMMAND ${CMAKE_CTEST_COMMAND} --verbose)
