###############
# Build tests
###############

################
# Add tests to ctest
################

ADD_EXECUTABLE(shuffile_test test1.c)
TARGET_LINK_LIBRARIES(shuffile_test ${SHUFFILE_EXTERNAL_LIBS} shuffile)
#ADD_TEST(NAME shuffile_test COMMAND ${MPIEXEC} -n6 -N3 -m block ./shuffile_test)
SHUFFILE_ADD_TEST(shuffile_test 256 "")


####################
# make a verbose "test" target named "check"
####################

ADD_CUSTOM_TARGET(check COMMAND ${CMAKE_CTEST_COMMAND} --verbose)
