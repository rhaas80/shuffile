###############
# Build tests
###############

################
# Add tests to ctest
################

ADD_EXECUTABLE(shuffile_test test1.c)
TARGET_LINK_LIBRARIES(shuffile_test ${SHUFFILE_EXTERNAL_LIBS} shuffile)
ADD_TEST(NAME shuffile_test COMMAND ${MPIEXEC} -n6 -N3 -m block ./shuffile_test)
#ADD_TEST(NAME shuffile_test COMMAND ${MPIEXEC} ${MPIEXEC_NUMPROC_FLAG} 4 ./shuffile_test)


####################
# make a verbose "test" target named "check"
####################

ADD_CUSTOM_TARGET(check COMMAND ${CMAKE_CTEST_COMMAND} --verbose)
