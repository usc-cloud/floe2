/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package edu.usc.pgroup.floe.thriftgen;


/**
 * Enum for App Status.
 */
public enum AppStatus implements org.apache.thrift.TEnum {
  /**
   * Application status indicating that this is a new application
   * request and the request has been received.
   */
  NEW_DEPLOYMENT_REQ_RECEIVED(0),
  /**
   * Application status indicating that currently the application is
   * being scheduled.
   */
  SCHEDULING(1),
  /**
   * Status to indicate that the scheduling has been completed.
   */
  SCHEDULING_COMPLETED(2),
  /**
   * Application status indicating that the application has been
   * scheduled and the corresponding flakes are being deployed. Same
   * status is used when the flakes are being updated.
   * This involves launching flakes, establishing socket connections.
   */
  UPDATING_FLAKES(3),
  /**
   * Status indicating that all flake updates have been completed.
   */
  UPDATING_FLAKES_COMPLETED(4),
  /**
   * Application status indicating that the flakes has been deployed
   * and the corresponding pellets are being created.
   * THIS SHOULD NOT START THE PELLETS. JUST CREATE THEM.
   * when creating, removing pellets. or changing alternates.
   */
  UPDATING_PELLETS(5),
  /**
   * Status indicating that the all pellet creation/deletion/updates have
   * been completed.
   */
  UPDATING_PELLETS_COMPLETED(6),
  /**
   * Status indicating that the application is ready to be started.
   */
  READY(7),
  /**
   * Status indicating that the application is running. And no update
   * requests are pending.
   * New requests will be accepted only when in this state.
   */
  RUNNING(8),
  /**
   * Status to indicate that a new update request has been received.
   */
  REDEPLOYMENT_REQ_RECEIVED(9),
  /**
   * Status to indicate that the system is Starting pellets.
   */
  STARTING_PELLETS(10),
  TERMINATED(11);

  private final int value;

  private AppStatus(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static AppStatus findByValue(int value) { 
    switch (value) {
      case 0:
        return NEW_DEPLOYMENT_REQ_RECEIVED;
      case 1:
        return SCHEDULING;
      case 2:
        return SCHEDULING_COMPLETED;
      case 3:
        return UPDATING_FLAKES;
      case 4:
        return UPDATING_FLAKES_COMPLETED;
      case 5:
        return UPDATING_PELLETS;
      case 6:
        return UPDATING_PELLETS_COMPLETED;
      case 7:
        return READY;
      case 8:
        return RUNNING;
      case 9:
        return REDEPLOYMENT_REQ_RECEIVED;
      case 10:
        return STARTING_PELLETS;
      case 11:
        return TERMINATED;
      default:
        return null;
    }
  }
}
