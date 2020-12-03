package com.kafkaLearning.constants

import java.util.Calendar

import org.apache.spark.sql.functions.current_timestamp

object CONSTANTS {


  //sql server connection constant table names
  final val site_tbl_name = "Sites"
  final val blast_tbl_name="Blasts"
  final val imported_drill_plan_tbl_name="ImportedDrillPlans"
  final val holes_tbl_name="Holes"
  final val name="Unknown"
  final val HDMeasurement_tbl_name = "HoleMeasurements"

  final val row_tbl_name = "RowTable"
  final val actual_decks_tbl = "ActualDecks"
  final val design_decks_tbl = "DesignDecks"
  final val adjusted_design_decks_tbl = "AdjustedDesignDecks"
  final val actual_holes_bulk = "ActualHolesBulk"
  final  val blastbulk_tbl = "BlastBulk"
  final val design_holes_bulk="DesignHolesBulk"
  final val holes_tag_details="HolesTagDetails"
  final val adjusted_DesignHoles_Bulk="AdjustedDesignHolesBulk"
  final val products_tbl_name="Products"
  final val threshold_tbl_name="Thresholds"
  //final val holes_geometry_tbl_name="HolesGeometry"
  final val sm_site_infos_tbl_name="SMSiteInfos"
  final  val blastbulkv3_tbl = "BlastBulkV3"
  final  val blastInertv3_tbl = "BlastInertV3"

  //temporary sql table names
  final val holes_temp_tbl_name="WrkHolesHoles"
  final val temp_row_tbl_name = "WrkHolesTempRowTable"
  final val wrk_row_tbl_name = "WrkHolesRowTable"

  final val colNameId="Id"
  final val colNameHoleId="HoleId"
  final val colNameHoleGuid="HoleGuid"
  final val colNameBlastId="BlastId"
  final val colNameDrillPlanGuid="DrillPlanGuid"

  final val AuditTableHoleValue="holes"
  // final val AuditTableEventType="Cosmos.Holes.Contracts.Events.HoleSavedV1"


  final val modeAppend="append"
  final val modeOverwrite="overwrite"

  val SystemUsername=System.getProperty("user.name")
  val siteIDCosmosIntegration=1

}
