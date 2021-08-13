#########################
# Stage 1: sbt assembly #
#########################

FROM mozilla/sbt:8u232_1.4.9 AS assembly
WORKDIR /home
COPY . .
RUN sbt assembly

#######################
# Stage 2a: packaging #
#######################

FROM flink:1.11.2-scala_2.12 AS package
COPY --from=assembly /home/lib/sync_job.jar /opt/flink/usrlib/

###################################################
# Stage 2b: packaging in CI (jar already present) #
###################################################

FROM flink:1.11.2-scala_2.12 AS package-ci
ADD lib/sync_job.jar /opt/flink/usrlib/
