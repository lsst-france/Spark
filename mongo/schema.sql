CREATE TABLE `ForcedSource` (
  `deepSourceId` bigint(20) NOT NULL,
  `scienceCcdExposureId` bigint(20) NOT NULL,
  `psfFlux` float DEFAULT NULL,
  `psfFluxSigma` float DEFAULT NULL,
  `flagBadMeasCentroid` bit(1) NOT NULL,
  `flagPixEdge` bit(1) NOT NULL,
  `flagPixInterpAny` bit(1) NOT NULL,
  `flagPixInterpCen` bit(1) NOT NULL,
  `flagPixSaturAny` bit(1) NOT NULL,
  `flagPixSaturCen` bit(1) NOT NULL,
  `flagBadPsfFlux` bit(1) NOT NULL,
  `chunkId` int(11) NOT NULL,
  `subChunkId` int(11) NOT NULL,
  PRIMARY KEY (`deepSourceId`,`scienceCcdExposureId`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;



CREATE TABLE `Object` (
  `deepSourceId` bigint(20) NOT NULL,
  `ra` double NOT NULL,
  `decl` double NOT NULL,
  `raVar` double DEFAULT NULL,
  `declVar` double DEFAULT NULL,
  `radeclCov` double DEFAULT NULL,
  `chunkId` int(11) NOT NULL,
  `subChunkId` int(11) NOT NULL,
  `u_psfFlux` double DEFAULT NULL,
  `u_psfFluxSigma` double DEFAULT NULL,
  `u_apFlux` double DEFAULT NULL,
  `u_apFluxSigma` double DEFAULT NULL,
  `u_modelFlux` double DEFAULT NULL,
  `u_modelFluxSigma` double DEFAULT NULL,
  `u_instFlux` double DEFAULT NULL,
  `u_instFluxSigma` double DEFAULT NULL,
  `u_apCorrection` double DEFAULT NULL,
  `u_apCorrectionSigma` double DEFAULT NULL,
  `u_shapeIx` double DEFAULT NULL,
  `u_shapeIy` double DEFAULT NULL,
  `u_shapeIxVar` double DEFAULT NULL,
  `u_shapeIyVar` double DEFAULT NULL,
  `u_shapeIxIyCov` double DEFAULT NULL,
  `u_shapeIxx` double DEFAULT NULL,
  `u_shapeIyy` double DEFAULT NULL,
  `u_shapeIxy` double DEFAULT NULL,
  `u_shapeIxxVar` double DEFAULT NULL,
  `u_shapeIyyVar` double DEFAULT NULL,
  `u_shapeIxyVar` double DEFAULT NULL,
  `u_shapeIxxIyyCov` double DEFAULT NULL,
  `u_shapeIxxIxyCov` double DEFAULT NULL,
  `u_shapeIyyIxyCov` double DEFAULT NULL,
  `u_extendedness` double DEFAULT NULL,
  `u_flagNegative` bit(1) DEFAULT NULL,
  `u_flagBadMeasCentroid` bit(1) DEFAULT NULL,
  `u_flagPixEdge` bit(1) DEFAULT NULL,
  `u_flagPixInterpAny` bit(1) DEFAULT NULL,
  `u_flagPixInterpCen` bit(1) DEFAULT NULL,
  `u_flagPixSaturAny` bit(1) DEFAULT NULL,
  `u_flagPixSaturCen` bit(1) DEFAULT NULL,
  `u_flagBadPsfFlux` bit(1) DEFAULT NULL,
  `u_flagBadApFlux` bit(1) DEFAULT NULL,
  `u_flagBadModelFlux` bit(1) DEFAULT NULL,
  `u_flagBadInstFlux` bit(1) DEFAULT NULL,
  `u_flagBadCentroid` bit(1) DEFAULT NULL,
  `u_flagBadShape` bit(1) DEFAULT NULL,
  `g_psfFlux` double DEFAULT NULL,
  `g_psfFluxSigma` double DEFAULT NULL,
  `g_apFlux` double DEFAULT NULL,
  `g_apFluxSigma` double DEFAULT NULL,
  `g_modelFlux` double DEFAULT NULL,
  `g_modelFluxSigma` double DEFAULT NULL,
  `g_instFlux` double DEFAULT NULL,
  `g_instFluxSigma` double DEFAULT NULL,
  `g_apCorrection` double DEFAULT NULL,
  `g_apCorrectionSigma` double DEFAULT NULL,
  `g_shapeIx` double DEFAULT NULL,
  `g_shapeIy` double DEFAULT NULL,
  `g_shapeIxVar` double DEFAULT NULL,
  `g_shapeIyVar` double DEFAULT NULL,
  `g_shapeIxIyCov` double DEFAULT NULL,
  `g_shapeIxx` double DEFAULT NULL,
  `g_shapeIyy` double DEFAULT NULL,
  `g_shapeIxy` double DEFAULT NULL,
  `g_shapeIxxVar` double DEFAULT NULL,
  `g_shapeIyyVar` double DEFAULT NULL,
  `g_shapeIxyVar` double DEFAULT NULL,
  `g_shapeIxxIyyCov` double DEFAULT NULL,
  `g_shapeIxxIxyCov` double DEFAULT NULL,
  `g_shapeIyyIxyCov` double DEFAULT NULL,
  `g_extendedness` double DEFAULT NULL,
  `g_flagNegative` bit(1) DEFAULT NULL,
  `g_flagBadMeasCentroid` bit(1) DEFAULT NULL,
  `g_flagPixEdge` bit(1) DEFAULT NULL,
  `g_flagPixInterpAny` bit(1) DEFAULT NULL,
  `g_flagPixInterpCen` bit(1) DEFAULT NULL,
  `g_flagPixSaturAny` bit(1) DEFAULT NULL,
  `g_flagPixSaturCen` bit(1) DEFAULT NULL,
  `g_flagBadPsfFlux` bit(1) DEFAULT NULL,
  `g_flagBadApFlux` bit(1) DEFAULT NULL,
  `g_flagBadModelFlux` bit(1) DEFAULT NULL,
  `g_flagBadInstFlux` bit(1) DEFAULT NULL,
  `g_flagBadCentroid` bit(1) DEFAULT NULL,
  `g_flagBadShape` bit(1) DEFAULT NULL,
  `r_psfFlux` double DEFAULT NULL,
  `r_psfFluxSigma` double DEFAULT NULL,
  `r_apFlux` double DEFAULT NULL,
  `r_apFluxSigma` double DEFAULT NULL,
  `r_modelFlux` double DEFAULT NULL,
  `r_modelFluxSigma` double DEFAULT NULL,
  `r_instFlux` double DEFAULT NULL,
  `r_instFluxSigma` double DEFAULT NULL,
  `r_apCorrection` double DEFAULT NULL,
  `r_apCorrectionSigma` double DEFAULT NULL,
  `r_shapeIx` double DEFAULT NULL,
  `r_shapeIy` double DEFAULT NULL,
  `r_shapeIxVar` double DEFAULT NULL,
  `r_shapeIyVar` double DEFAULT NULL,
  `r_shapeIxIyCov` double DEFAULT NULL,
  `r_shapeIxx` double DEFAULT NULL,
  `r_shapeIyy` double DEFAULT NULL,
  `r_shapeIxy` double DEFAULT NULL,
  `r_shapeIxxVar` double DEFAULT NULL,
  `r_shapeIyyVar` double DEFAULT NULL,
  `r_shapeIxyVar` double DEFAULT NULL,
  `r_shapeIxxIyyCov` double DEFAULT NULL,
  `r_shapeIxxIxyCov` double DEFAULT NULL,
  `r_shapeIyyIxyCov` double DEFAULT NULL,
  `r_extendedness` double DEFAULT NULL,
  `r_flagNegative` bit(1) DEFAULT NULL,
  `r_flagBadMeasCentroid` bit(1) DEFAULT NULL,
  `r_flagPixEdge` bit(1) DEFAULT NULL,
  `r_flagPixInterpAny` bit(1) DEFAULT NULL,
  `r_flagPixInterpCen` bit(1) DEFAULT NULL,
  `r_flagPixSaturAny` bit(1) DEFAULT NULL,
  `r_flagPixSaturCen` bit(1) DEFAULT NULL,
  `r_flagBadPsfFlux` bit(1) DEFAULT NULL,
  `r_flagBadApFlux` bit(1) DEFAULT NULL,
  `r_flagBadModelFlux` bit(1) DEFAULT NULL,
  `r_flagBadInstFlux` bit(1) DEFAULT NULL,
  `r_flagBadCentroid` bit(1) DEFAULT NULL,
  `r_flagBadShape` bit(1) DEFAULT NULL,
  `i_psfFlux` double DEFAULT NULL,
  `i_psfFluxSigma` double DEFAULT NULL,
  `i_apFlux` double DEFAULT NULL,
  `i_apFluxSigma` double DEFAULT NULL,
  `i_modelFlux` double DEFAULT NULL,
  `i_modelFluxSigma` double DEFAULT NULL,
  `i_instFlux` double DEFAULT NULL,
  `i_instFluxSigma` double DEFAULT NULL,
  `i_apCorrection` double DEFAULT NULL,
  `i_apCorrectionSigma` double DEFAULT NULL,
  `i_shapeIx` double DEFAULT NULL,
  `i_shapeIy` double DEFAULT NULL,
  `i_shapeIxVar` double DEFAULT NULL,
  `i_shapeIyVar` double DEFAULT NULL,
  `i_shapeIxIyCov` double DEFAULT NULL,
  `i_shapeIxx` double DEFAULT NULL,
  `i_shapeIyy` double DEFAULT NULL,
  `i_shapeIxy` double DEFAULT NULL,
  `i_shapeIxxVar` double DEFAULT NULL,
  `i_shapeIyyVar` double DEFAULT NULL,
  `i_shapeIxyVar` double DEFAULT NULL,
  `i_shapeIxxIyyCov` double DEFAULT NULL,
  `i_shapeIxxIxyCov` double DEFAULT NULL,
  `i_shapeIyyIxyCov` double DEFAULT NULL,
  `i_extendedness` double DEFAULT NULL,
  `i_flagNegative` bit(1) DEFAULT NULL,
  `i_flagBadMeasCentroid` bit(1) DEFAULT NULL,
  `i_flagPixEdge` bit(1) DEFAULT NULL,
  `i_flagPixInterpAny` bit(1) DEFAULT NULL,
  `i_flagPixInterpCen` bit(1) DEFAULT NULL,
  `i_flagPixSaturAny` bit(1) DEFAULT NULL,
  `i_flagPixSaturCen` bit(1) DEFAULT NULL,
  `i_flagBadPsfFlux` bit(1) DEFAULT NULL,
  `i_flagBadApFlux` bit(1) DEFAULT NULL,
  `i_flagBadModelFlux` bit(1) DEFAULT NULL,
  `i_flagBadInstFlux` bit(1) DEFAULT NULL,
  `i_flagBadCentroid` bit(1) DEFAULT NULL,
  `i_flagBadShape` bit(1) DEFAULT NULL,
  `z_psfFlux` double DEFAULT NULL,
  `z_psfFluxSigma` double DEFAULT NULL,
  `z_apFlux` double DEFAULT NULL,
  `z_apFluxSigma` double DEFAULT NULL,
  `z_modelFlux` double DEFAULT NULL,
  `z_modelFluxSigma` double DEFAULT NULL,
  `z_instFlux` double DEFAULT NULL,
  `z_instFluxSigma` double DEFAULT NULL,
  `z_apCorrection` double DEFAULT NULL,
  `z_apCorrectionSigma` double DEFAULT NULL,
  `z_shapeIx` double DEFAULT NULL,
  `z_shapeIy` double DEFAULT NULL,
  `z_shapeIxVar` double DEFAULT NULL,
  `z_shapeIyVar` double DEFAULT NULL,
  `z_shapeIxIyCov` double DEFAULT NULL,
  `z_shapeIxx` double DEFAULT NULL,
  `z_shapeIyy` double DEFAULT NULL,
  `z_shapeIxy` double DEFAULT NULL,
  `z_shapeIxxVar` double DEFAULT NULL,
  `z_shapeIyyVar` double DEFAULT NULL,
  `z_shapeIxyVar` double DEFAULT NULL,
  `z_shapeIxxIyyCov` double DEFAULT NULL,
  `z_shapeIxxIxyCov` double DEFAULT NULL,
  `z_shapeIyyIxyCov` double DEFAULT NULL,
  `z_extendedness` double DEFAULT NULL,
  `z_flagNegative` bit(1) DEFAULT NULL,
  `z_flagBadMeasCentroid` bit(1) DEFAULT NULL,
  `z_flagPixEdge` bit(1) DEFAULT NULL,
  `z_flagPixInterpAny` bit(1) DEFAULT NULL,
  `z_flagPixInterpCen` bit(1) DEFAULT NULL,
  `z_flagPixSaturAny` bit(1) DEFAULT NULL,
  `z_flagPixSaturCen` bit(1) DEFAULT NULL,
  `z_flagBadPsfFlux` bit(1) DEFAULT NULL,
  `z_flagBadApFlux` bit(1) DEFAULT NULL,
  `z_flagBadModelFlux` bit(1) DEFAULT NULL,
  `z_flagBadInstFlux` bit(1) DEFAULT NULL,
  `z_flagBadCentroid` bit(1) DEFAULT NULL,
  `z_flagBadShape` bit(1) DEFAULT NULL,
  `y_psfFlux` double DEFAULT NULL,
  `y_psfFluxSigma` double DEFAULT NULL,
  `y_apFlux` double DEFAULT NULL,
  `y_apFluxSigma` double DEFAULT NULL,
  `y_modelFlux` double DEFAULT NULL,
  `y_modelFluxSigma` double DEFAULT NULL,
  `y_instFlux` double DEFAULT NULL,
  `y_instFluxSigma` double DEFAULT NULL,
  `y_apCorrection` double DEFAULT NULL,
  `y_apCorrectionSigma` double DEFAULT NULL,
  `y_shapeIx` double DEFAULT NULL,
  `y_shapeIy` double DEFAULT NULL,
  `y_shapeIxVar` double DEFAULT NULL,
  `y_shapeIyVar` double DEFAULT NULL,
  `y_shapeIxIyCov` double DEFAULT NULL,
  `y_shapeIxx` double DEFAULT NULL,
  `y_shapeIyy` double DEFAULT NULL,
  `y_shapeIxy` double DEFAULT NULL,
  `y_shapeIxxVar` double DEFAULT NULL,
  `y_shapeIyyVar` double DEFAULT NULL,
  `y_shapeIxyVar` double DEFAULT NULL,
  `y_shapeIxxIyyCov` double DEFAULT NULL,
  `y_shapeIxxIxyCov` double DEFAULT NULL,
  `y_shapeIyyIxyCov` double DEFAULT NULL,
  `y_extendedness` double DEFAULT NULL,
  `y_flagNegative` bit(1) DEFAULT NULL,
  `y_flagBadMeasCentroid` bit(1) DEFAULT NULL,
  `y_flagPixEdge` bit(1) DEFAULT NULL,
  `y_flagPixInterpAny` bit(1) DEFAULT NULL,
  `y_flagPixInterpCen` bit(1) DEFAULT NULL,
  `y_flagPixSaturAny` bit(1) DEFAULT NULL,
  `y_flagPixSaturCen` bit(1) DEFAULT NULL,
  `y_flagBadPsfFlux` bit(1) DEFAULT NULL,
  `y_flagBadApFlux` bit(1) DEFAULT NULL,
  `y_flagBadModelFlux` bit(1) DEFAULT NULL,
  `y_flagBadInstFlux` bit(1) DEFAULT NULL,
  `y_flagBadCentroid` bit(1) DEFAULT NULL,
  `y_flagBadShape` bit(1) DEFAULT NULL,
  PRIMARY KEY (`deepSourceId`),
  KEY `subChunkId` (`subChunkId`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

CREATE TABLE `ObjectFullOverlap_0` (
  `deepSourceId` bigint(20) NOT NULL,
  `ra` double NOT NULL,
  `decl` double NOT NULL,
  `raVar` double DEFAULT NULL,
  `declVar` double DEFAULT NULL,
  `radeclCov` double DEFAULT NULL,
  `chunkId` int(11) NOT NULL,
  `subChunkId` int(11) NOT NULL,
  `u_psfFlux` double DEFAULT NULL,
  `u_psfFluxSigma` double DEFAULT NULL,
  `u_apFlux` double DEFAULT NULL,
  `u_apFluxSigma` double DEFAULT NULL,
  `u_modelFlux` double DEFAULT NULL,
  `u_modelFluxSigma` double DEFAULT NULL,
  `u_instFlux` double DEFAULT NULL,
  `u_instFluxSigma` double DEFAULT NULL,
  `u_apCorrection` double DEFAULT NULL,
  `u_apCorrectionSigma` double DEFAULT NULL,
  `u_shapeIx` double DEFAULT NULL,
  `u_shapeIy` double DEFAULT NULL,
  `u_shapeIxVar` double DEFAULT NULL,
  `u_shapeIyVar` double DEFAULT NULL,
  `u_shapeIxIyCov` double DEFAULT NULL,
  `u_shapeIxx` double DEFAULT NULL,
  `u_shapeIyy` double DEFAULT NULL,
  `u_shapeIxy` double DEFAULT NULL,
  `u_shapeIxxVar` double DEFAULT NULL,
  `u_shapeIyyVar` double DEFAULT NULL,
  `u_shapeIxyVar` double DEFAULT NULL,
  `u_shapeIxxIyyCov` double DEFAULT NULL,
  `u_shapeIxxIxyCov` double DEFAULT NULL,
  `u_shapeIyyIxyCov` double DEFAULT NULL,
  `u_extendedness` double DEFAULT NULL,
  `u_flagNegative` bit(1) DEFAULT NULL,
  `u_flagBadMeasCentroid` bit(1) DEFAULT NULL,
  `u_flagPixEdge` bit(1) DEFAULT NULL,
  `u_flagPixInterpAny` bit(1) DEFAULT NULL,
  `u_flagPixInterpCen` bit(1) DEFAULT NULL,
  `u_flagPixSaturAny` bit(1) DEFAULT NULL,
  `u_flagPixSaturCen` bit(1) DEFAULT NULL,
  `u_flagBadPsfFlux` bit(1) DEFAULT NULL,
  `u_flagBadApFlux` bit(1) DEFAULT NULL,
  `u_flagBadModelFlux` bit(1) DEFAULT NULL,
  `u_flagBadInstFlux` bit(1) DEFAULT NULL,
  `u_flagBadCentroid` bit(1) DEFAULT NULL,
  `u_flagBadShape` bit(1) DEFAULT NULL,
  `g_psfFlux` double DEFAULT NULL,
  `g_psfFluxSigma` double DEFAULT NULL,
  `g_apFlux` double DEFAULT NULL,
  `g_apFluxSigma` double DEFAULT NULL,
  `g_modelFlux` double DEFAULT NULL,
  `g_modelFluxSigma` double DEFAULT NULL,
  `g_instFlux` double DEFAULT NULL,
  `g_instFluxSigma` double DEFAULT NULL,
  `g_apCorrection` double DEFAULT NULL,
  `g_apCorrectionSigma` double DEFAULT NULL,
  `g_shapeIx` double DEFAULT NULL,
  `g_shapeIy` double DEFAULT NULL,
  `g_shapeIxVar` double DEFAULT NULL,
  `g_shapeIyVar` double DEFAULT NULL,
  `g_shapeIxIyCov` double DEFAULT NULL,
  `g_shapeIxx` double DEFAULT NULL,
  `g_shapeIyy` double DEFAULT NULL,
  `g_shapeIxy` double DEFAULT NULL,
  `g_shapeIxxVar` double DEFAULT NULL,
  `g_shapeIyyVar` double DEFAULT NULL,
  `g_shapeIxyVar` double DEFAULT NULL,
  `g_shapeIxxIyyCov` double DEFAULT NULL,
  `g_shapeIxxIxyCov` double DEFAULT NULL,
  `g_shapeIyyIxyCov` double DEFAULT NULL,
  `g_extendedness` double DEFAULT NULL,
  `g_flagNegative` bit(1) DEFAULT NULL,
  `g_flagBadMeasCentroid` bit(1) DEFAULT NULL,
  `g_flagPixEdge` bit(1) DEFAULT NULL,
  `g_flagPixInterpAny` bit(1) DEFAULT NULL,
  `g_flagPixInterpCen` bit(1) DEFAULT NULL,
  `g_flagPixSaturAny` bit(1) DEFAULT NULL,
  `g_flagPixSaturCen` bit(1) DEFAULT NULL,
  `g_flagBadPsfFlux` bit(1) DEFAULT NULL,
  `g_flagBadApFlux` bit(1) DEFAULT NULL,
  `g_flagBadModelFlux` bit(1) DEFAULT NULL,
  `g_flagBadInstFlux` bit(1) DEFAULT NULL,
  `g_flagBadCentroid` bit(1) DEFAULT NULL,
  `g_flagBadShape` bit(1) DEFAULT NULL,
  `r_psfFlux` double DEFAULT NULL,
  `r_psfFluxSigma` double DEFAULT NULL,
  `r_apFlux` double DEFAULT NULL,
  `r_apFluxSigma` double DEFAULT NULL,
  `r_modelFlux` double DEFAULT NULL,
  `r_modelFluxSigma` double DEFAULT NULL,
  `r_instFlux` double DEFAULT NULL,
  `r_instFluxSigma` double DEFAULT NULL,
  `r_apCorrection` double DEFAULT NULL,
  `r_apCorrectionSigma` double DEFAULT NULL,
  `r_shapeIx` double DEFAULT NULL,
  `r_shapeIy` double DEFAULT NULL,
  `r_shapeIxVar` double DEFAULT NULL,
  `r_shapeIyVar` double DEFAULT NULL,
  `r_shapeIxIyCov` double DEFAULT NULL,
  `r_shapeIxx` double DEFAULT NULL,
  `r_shapeIyy` double DEFAULT NULL,
  `r_shapeIxy` double DEFAULT NULL,
  `r_shapeIxxVar` double DEFAULT NULL,
  `r_shapeIyyVar` double DEFAULT NULL,
  `r_shapeIxyVar` double DEFAULT NULL,
  `r_shapeIxxIyyCov` double DEFAULT NULL,
  `r_shapeIxxIxyCov` double DEFAULT NULL,
  `r_shapeIyyIxyCov` double DEFAULT NULL,
  `r_extendedness` double DEFAULT NULL,
  `r_flagNegative` bit(1) DEFAULT NULL,
  `r_flagBadMeasCentroid` bit(1) DEFAULT NULL,
  `r_flagPixEdge` bit(1) DEFAULT NULL,
  `r_flagPixInterpAny` bit(1) DEFAULT NULL,
  `r_flagPixInterpCen` bit(1) DEFAULT NULL,
  `r_flagPixSaturAny` bit(1) DEFAULT NULL,
  `r_flagPixSaturCen` bit(1) DEFAULT NULL,
  `r_flagBadPsfFlux` bit(1) DEFAULT NULL,
  `r_flagBadApFlux` bit(1) DEFAULT NULL,
  `r_flagBadModelFlux` bit(1) DEFAULT NULL,
  `r_flagBadInstFlux` bit(1) DEFAULT NULL,
  `r_flagBadCentroid` bit(1) DEFAULT NULL,
  `r_flagBadShape` bit(1) DEFAULT NULL,
  `i_psfFlux` double DEFAULT NULL,
  `i_psfFluxSigma` double DEFAULT NULL,
  `i_apFlux` double DEFAULT NULL,
  `i_apFluxSigma` double DEFAULT NULL,
  `i_modelFlux` double DEFAULT NULL,
  `i_modelFluxSigma` double DEFAULT NULL,
  `i_instFlux` double DEFAULT NULL,
  `i_instFluxSigma` double DEFAULT NULL,
  `i_apCorrection` double DEFAULT NULL,
  `i_apCorrectionSigma` double DEFAULT NULL,
  `i_shapeIx` double DEFAULT NULL,
  `i_shapeIy` double DEFAULT NULL,
  `i_shapeIxVar` double DEFAULT NULL,
  `i_shapeIyVar` double DEFAULT NULL,
  `i_shapeIxIyCov` double DEFAULT NULL,
  `i_shapeIxx` double DEFAULT NULL,
  `i_shapeIyy` double DEFAULT NULL,
  `i_shapeIxy` double DEFAULT NULL,
  `i_shapeIxxVar` double DEFAULT NULL,
  `i_shapeIyyVar` double DEFAULT NULL,
  `i_shapeIxyVar` double DEFAULT NULL,
  `i_shapeIxxIyyCov` double DEFAULT NULL,
  `i_shapeIxxIxyCov` double DEFAULT NULL,
  `i_shapeIyyIxyCov` double DEFAULT NULL,
  `i_extendedness` double DEFAULT NULL,
  `i_flagNegative` bit(1) DEFAULT NULL,
  `i_flagBadMeasCentroid` bit(1) DEFAULT NULL,
  `i_flagPixEdge` bit(1) DEFAULT NULL,
  `i_flagPixInterpAny` bit(1) DEFAULT NULL,
  `i_flagPixInterpCen` bit(1) DEFAULT NULL,
  `i_flagPixSaturAny` bit(1) DEFAULT NULL,
  `i_flagPixSaturCen` bit(1) DEFAULT NULL,
  `i_flagBadPsfFlux` bit(1) DEFAULT NULL,
  `i_flagBadApFlux` bit(1) DEFAULT NULL,
  `i_flagBadModelFlux` bit(1) DEFAULT NULL,
  `i_flagBadInstFlux` bit(1) DEFAULT NULL,
  `i_flagBadCentroid` bit(1) DEFAULT NULL,
  `i_flagBadShape` bit(1) DEFAULT NULL,
  `z_psfFlux` double DEFAULT NULL,
  `z_psfFluxSigma` double DEFAULT NULL,
  `z_apFlux` double DEFAULT NULL,
  `z_apFluxSigma` double DEFAULT NULL,
  `z_modelFlux` double DEFAULT NULL,
  `z_modelFluxSigma` double DEFAULT NULL,
  `z_instFlux` double DEFAULT NULL,
  `z_instFluxSigma` double DEFAULT NULL,
  `z_apCorrection` double DEFAULT NULL,
  `z_apCorrectionSigma` double DEFAULT NULL,
  `z_shapeIx` double DEFAULT NULL,
  `z_shapeIy` double DEFAULT NULL,
  `z_shapeIxVar` double DEFAULT NULL,
  `z_shapeIyVar` double DEFAULT NULL,
  `z_shapeIxIyCov` double DEFAULT NULL,
  `z_shapeIxx` double DEFAULT NULL,
  `z_shapeIyy` double DEFAULT NULL,
  `z_shapeIxy` double DEFAULT NULL,
  `z_shapeIxxVar` double DEFAULT NULL,
  `z_shapeIyyVar` double DEFAULT NULL,
  `z_shapeIxyVar` double DEFAULT NULL,
  `z_shapeIxxIyyCov` double DEFAULT NULL,
  `z_shapeIxxIxyCov` double DEFAULT NULL,
  `z_shapeIyyIxyCov` double DEFAULT NULL,
  `z_extendedness` double DEFAULT NULL,
  `z_flagNegative` bit(1) DEFAULT NULL,
  `z_flagBadMeasCentroid` bit(1) DEFAULT NULL,
  `z_flagPixEdge` bit(1) DEFAULT NULL,
  `z_flagPixInterpAny` bit(1) DEFAULT NULL,
  `z_flagPixInterpCen` bit(1) DEFAULT NULL,
  `z_flagPixSaturAny` bit(1) DEFAULT NULL,
  `z_flagPixSaturCen` bit(1) DEFAULT NULL,
  `z_flagBadPsfFlux` bit(1) DEFAULT NULL,
  `z_flagBadApFlux` bit(1) DEFAULT NULL,
  `z_flagBadModelFlux` bit(1) DEFAULT NULL,
  `z_flagBadInstFlux` bit(1) DEFAULT NULL,
  `z_flagBadCentroid` bit(1) DEFAULT NULL,
  `z_flagBadShape` bit(1) DEFAULT NULL,
  `y_psfFlux` double DEFAULT NULL,
  `y_psfFluxSigma` double DEFAULT NULL,
  `y_apFlux` double DEFAULT NULL,
  `y_apFluxSigma` double DEFAULT NULL,
  `y_modelFlux` double DEFAULT NULL,
  `y_modelFluxSigma` double DEFAULT NULL,
  `y_instFlux` double DEFAULT NULL,
  `y_instFluxSigma` double DEFAULT NULL,
  `y_apCorrection` double DEFAULT NULL,
  `y_apCorrectionSigma` double DEFAULT NULL,
  `y_shapeIx` double DEFAULT NULL,
  `y_shapeIy` double DEFAULT NULL,
  `y_shapeIxVar` double DEFAULT NULL,
  `y_shapeIyVar` double DEFAULT NULL,
  `y_shapeIxIyCov` double DEFAULT NULL,
  `y_shapeIxx` double DEFAULT NULL,
  `y_shapeIyy` double DEFAULT NULL,
  `y_shapeIxy` double DEFAULT NULL,
  `y_shapeIxxVar` double DEFAULT NULL,
  `y_shapeIyyVar` double DEFAULT NULL,
  `y_shapeIxyVar` double DEFAULT NULL,
  `y_shapeIxxIyyCov` double DEFAULT NULL,
  `y_shapeIxxIxyCov` double DEFAULT NULL,
  `y_shapeIyyIxyCov` double DEFAULT NULL,
  `y_extendedness` double DEFAULT NULL,
  `y_flagNegative` bit(1) DEFAULT NULL,
  `y_flagBadMeasCentroid` bit(1) DEFAULT NULL,
  `y_flagPixEdge` bit(1) DEFAULT NULL,
  `y_flagPixInterpAny` bit(1) DEFAULT NULL,
  `y_flagPixInterpCen` bit(1) DEFAULT NULL,
  `y_flagPixSaturAny` bit(1) DEFAULT NULL,
  `y_flagPixSaturCen` bit(1) DEFAULT NULL,
  `y_flagBadPsfFlux` bit(1) DEFAULT NULL,
  `y_flagBadApFlux` bit(1) DEFAULT NULL,
  `y_flagBadModelFlux` bit(1) DEFAULT NULL,
  `y_flagBadInstFlux` bit(1) DEFAULT NULL,
  `y_flagBadCentroid` bit(1) DEFAULT NULL,
  `y_flagBadShape` bit(1) DEFAULT NULL,
  KEY `subChunkId` (`subChunkId`),
  KEY `deepSourceId` (`deepSourceId`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

CREATE TABLE `Source` (
  `id` bigint(20) NOT NULL,
  `coord_ra` double DEFAULT NULL,
  `coord_decl` double DEFAULT NULL,
  `coord_htmId20` bigint(20) DEFAULT NULL,
  `parent` bigint(20) DEFAULT NULL,
  `flags_badcentroid` bit(1) NOT NULL,
  `centroid_sdss_x` double DEFAULT NULL,
  `centroid_sdss_y` double DEFAULT NULL,
  `centroid_sdss_xVar` double DEFAULT NULL,
  `centroid_sdss_xyCov` double DEFAULT NULL,
  `centroid_sdss_yVar` double DEFAULT NULL,
  `centroid_sdss_flags` bit(1) NOT NULL,
  `flags_pixel_edge` bit(1) NOT NULL,
  `flags_pixel_interpolated_any` bit(1) NOT NULL,
  `flags_pixel_interpolated_center` bit(1) NOT NULL,
  `flags_pixel_saturated_any` bit(1) NOT NULL,
  `flags_pixel_saturated_center` bit(1) NOT NULL,
  `flags_pixel_cr_any` bit(1) NOT NULL,
  `flags_pixel_cr_center` bit(1) NOT NULL,
  `centroid_naive_x` double DEFAULT NULL,
  `centroid_naive_y` double DEFAULT NULL,
  `centroid_naive_xVar` double DEFAULT NULL,
  `centroid_naive_xyCov` double DEFAULT NULL,
  `centroid_naive_yVar` double DEFAULT NULL,
  `centroid_naive_flags` bit(1) NOT NULL,
  `centroid_gaussian_x` double DEFAULT NULL,
  `centroid_gaussian_y` double DEFAULT NULL,
  `centroid_gaussian_xVar` double DEFAULT NULL,
  `centroid_gaussian_xyCov` double DEFAULT NULL,
  `centroid_gaussian_yVar` double DEFAULT NULL,
  `centroid_gaussian_flags` bit(1) NOT NULL,
  `shape_sdss_Ixx` double DEFAULT NULL,
  `shape_sdss_Iyy` double DEFAULT NULL,
  `shape_sdss_Ixy` double DEFAULT NULL,
  `shape_sdss_IxxVar` double DEFAULT NULL,
  `shape_sdss_IxxIyyCov` double DEFAULT NULL,
  `shape_sdss_IxxIxyCov` double DEFAULT NULL,
  `shape_sdss_IyyVar` double DEFAULT NULL,
  `shape_sdss_IyyIxyCov` double DEFAULT NULL,
  `shape_sdss_IxyVar` double DEFAULT NULL,
  `shape_sdss_flags` bit(1) NOT NULL,
  `shape_sdss_centroid_x` double DEFAULT NULL,
  `shape_sdss_centroid_y` double DEFAULT NULL,
  `shape_sdss_centroid_xVar` double DEFAULT NULL,
  `shape_sdss_centroid_xyCov` double DEFAULT NULL,
  `shape_sdss_centroid_yVar` double DEFAULT NULL,
  `shape_sdss_centroid_flags` bit(1) NOT NULL,
  `shape_sdss_flags_unweightedbad` bit(1) NOT NULL,
  `shape_sdss_flags_unweighted` bit(1) NOT NULL,
  `shape_sdss_flags_shift` bit(1) NOT NULL,
  `shape_sdss_flags_maxiter` bit(1) NOT NULL,
  `flux_psf` double DEFAULT NULL,
  `flux_psf_err` double DEFAULT NULL,
  `flux_psf_flags` bit(1) NOT NULL,
  `flux_psf_psffactor` float DEFAULT NULL,
  `flux_psf_flags_psffactor` bit(1) NOT NULL,
  `flux_psf_flags_badcorr` bit(1) NOT NULL,
  `flux_naive` double DEFAULT NULL,
  `flux_naive_err` double DEFAULT NULL,
  `flux_naive_flags` bit(1) NOT NULL,
  `flux_gaussian` double DEFAULT NULL,
  `flux_gaussian_err` double DEFAULT NULL,
  `flux_gaussian_flags` bit(1) NOT NULL,
  `flux_gaussian_psffactor` float DEFAULT NULL,
  `flux_gaussian_flags_psffactor` bit(1) NOT NULL,
  `flux_gaussian_flags_badcorr` bit(1) NOT NULL,
  `flux_sinc` double DEFAULT NULL,
  `flux_sinc_err` double DEFAULT NULL,
  `flux_sinc_flags` bit(1) NOT NULL,
  `centroid_record_x` double DEFAULT NULL,
  `centroid_record_y` double DEFAULT NULL,
  `classification_extendedness` double DEFAULT NULL,
  `aperturecorrection` double DEFAULT NULL,
  `aperturecorrection_err` double DEFAULT NULL,
  `refFlux` double DEFAULT NULL,
  `refFlux_err` double DEFAULT NULL,
  `objectId` bigint(20) NOT NULL,
  `coord_raVar` double DEFAULT NULL,
  `coord_radeclCov` double DEFAULT NULL,
  `coord_declVar` double DEFAULT NULL,
  `exposure_id` bigint(20) NOT NULL,
  `exposure_filter_id` int(11) NOT NULL,
  `exposure_time` float DEFAULT NULL,
  `exposure_time_mid` double DEFAULT NULL,
  `cluster_id` bigint(20) DEFAULT NULL,
  `cluster_coord_ra` double DEFAULT NULL,
  `cluster_coord_decl` double DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `IDX_parent` (`parent`),
  KEY `IDX_exposure_id` (`exposure_id`),
  KEY `objectId` (`objectId`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

