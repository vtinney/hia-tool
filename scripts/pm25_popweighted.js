// =============================================================================
// pm25_popweighted.js
// -----------------------------------------------------------------------------
// Compute annual population-weighted PM2.5 per feature for 9 boundary sets.
//
// WHAT THIS DOES
//   For every year from CONFIG.startYear through CONFIG.endYear, and for each
//   of 3 pre-uploaded boundary FeatureCollections (Natural Earth countries,
//   Natural Earth states, and GHS_SMOD urban areas), this script:
//     1. Loads the van Donkelaar annual mean PM2.5 image from the sat-io
//        community mirror (GLOBAL-SATELLITE-PM25/ANNUAL).
//     2. Loads the sat-io WORLDPOP/agesex image for the same year and
//        mosaics it globally. This collection covers 2015-2030 annually.
//     3. Builds a pop_total band plus 20 age bands (age_0, age_1, age_5 .. age_90)
//        where each age_x = m_x + f_x, and pop_total = sum over all age bands.
//     4. Resamples the population stack onto the PM2.5 grid using
//        reduceResolution(Reducer.sum()) so that counts are preserved, then
//        reprojects onto the PM2.5 projection.
//     5. Computes pm25_x_pop = pm25 * pop_total per pixel.
//     6. reduceRegions with Reducer.sum() over [pop_total, age_*, pm25_x_pop]
//        AND a separate reduceRegions with Reducer.mean() over pm25 for the
//        unweighted reference mean, then joins by feature_id.
//     7. Exports one CSV per boundary set to Google Drive folder
//        'hia_tool_pm25' via Export.table.toDrive.
//
//   The final population-weighted mean (pm25_popweighted) is NOT computed here;
//   it is computed downstream in scripts/pm25_csv_to_parquet.py as
//   sum(pm25_x_pop) / sum(pop_total), which is mathematically identical to the
//   pixel-level weighted mean but avoids dividing zero totals on GEE.
//
// HOW TO RUN
//   1. Open https://code.earthengine.google.com/ signed in with a GEE-enabled
//      Google account that has read access to projects/hia-tool/assets/*.
//   2. Create a new script, paste the entire contents of this file, click Run.
//   3. Open the Tasks panel (right sidebar). 24 export tasks will be queued:
//        pm25_{boundary}_{year} for each of 3 boundaries × 8 years.
//   4. Click Run on each task. Individual year-tasks typically finish within
//      tens of minutes to a couple of hours.
//   5. When all tasks complete, download the CSVs from Google Drive folder
//      'hia_tool_pm25' into hia-tool/data/raw/pm25_csv/ and run
//      scripts/pm25_csv_to_parquet.py to convert them to Parquet.
//
//   Before running for the first time, follow the runbook at
//   docs/methods/pm25_gee_runbook.md to verify that the idField/nameField
//   assumptions in CONFIG.boundaries match your actual uploaded assets, and
//   that PM2.5 / WorldPop band names match the defaults used below.
//
// OUTPUTS
//   Drive folder : hia_tool_pm25
//   Files        : pm25_{ne_countries,ne_states,ghs_smod}_{2015..2022}.csv
//   Columns      : feature_id, name, year, pop_source_year, pop_total,
//                  pm25_x_pop, pm25_mean, age_0, age_1, age_5, ..., age_90
//                  (20 age bins total, covering 0-1 through 90+)
// =============================================================================

// Safe global extent that avoids lat ±90 edges where GEE's EPSG:4326
// "unable to transform edge" errors happen. WorldPop data is only defined
// to ~±85 anyway, and van Donkelaar V5GL04 stops at -55 / +70, so this is
// strictly within the valid region.
var VALID_BOUNDS = ee.Geometry.BBox(-180, -85, 180, 85);

// Explicit 0.01-degree EPSG:4326 grid pinned to the valid bounds.
// Origin (-180, 85), x-scale +0.01 (east), y-scale -0.01 (south).
// 36000 cols x 17000 rows total — exactly covers VALID_BOUNDS with no
// overshoot, so no reduceRegions pixel can land past the poles.
var GRID_CRS_TRANSFORM = [0.01, 0, -180, 0, -0.01, 85];

var CONFIG = {
  pm25Collection: 'projects/sat-io/open-datasets/GLOBAL-SATELLITE-PM25/ANNUAL',
  wpCollection:   'projects/sat-io/open-datasets/WORLDPOP/agesex',
  startYear:      2015,
  endYear:        2022,              // clipped to PM2.5 availability at runtime
  driveFolder:    'hia_tool_pm25',
  reduceScale:    1113.2,            // 0.01 deg ~ 1113 m; matches PM2.5 native grid
  tileScale:      16,                // bumped from 8 after earlier compute limits
  // idField/nameField values are the plan's defaults; verify via the Task 0
  // probe in docs/methods/pm25_gee_runbook.md before running for the first
  // time, and edit in place if any asset differs.
  boundaries: [
    // UNCOMMENT after GHS_SMOD re-run is complete:
    // {assetId: 'projects/hia-tool/assets/ne_countries', name: 'ne_countries', idField: 'ADM0_A3',   nameField: 'NAME'},
    // {assetId: 'projects/hia-tool/assets/ne_states',    name: 'ne_states',    idField: 'adm1_code', nameField: 'name'},
    // GHS_SMOD has ~10k+ urban features. batchSize splits it into chunks
    // per export task so reduceRegions doesn't time out. 500 keeps even
    // the most geometry-heavy batches under GEE's 25-min compute limit.
    {assetId: 'projects/hia-tool/assets/GHS_SMOD', name: 'ghs_smod', idField: 'ID_HDC_G0', nameField: 'UC_NM_MN', batchSize: 500},
  ],
  // 20 WorldPop 5-year age bins, labelled by lower bound.
  // age_0 = 0-1, age_1 = 1-4, age_5 = 5-9, ..., age_85 = 85-89, age_90 = 90+.
  // The sat-io WORLDPOP/agesex bands are lowercase zero-padded (m_00, f_05, ...);
  // prepAgeBands handles the padding when selecting.
  ageBins: [0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90],
};

// -----------------------------------------------------------------------------
// loadPM25(year) -> ee.Image with single band 'pm25'
// -----------------------------------------------------------------------------
function loadPM25(year) {
  // The sat-io GLOBAL-SATELLITE-PM25/ANNUAL collection has one annual image
  // per year. The single band is renamed to 'pm25' regardless of its native
  // name. Clipping to VALID_BOUNDS keeps the downstream reduceRegions from
  // asking GEE to transform coordinates past the poles.
  var col = ee.ImageCollection(CONFIG.pm25Collection)
    .filterDate(year + '-01-01', (year + 1) + '-01-01');
  var img = ee.Image(col.first()).rename(['pm25']).clip(VALID_BOUNDS);
  return img.set('year', year);
}

// -----------------------------------------------------------------------------
// loadWorldPop(year) -> ee.Image (mosaic) with bands m_00..m_90, f_00..f_90
// -----------------------------------------------------------------------------
function loadWorldPop(year) {
  // The sat-io WORLDPOP/agesex collection covers 2015-2030 annually, so no
  // fallback is needed for our year range. Each image in the collection is a
  // single country-year; mosaic() stitches them into a global image. Clip to
  // VALID_BOUNDS for the same reason as pm25. setDefaultProjection() gives
  // the mosaic a 100m EPSG:4326 pyramid so reduceRegions knows how to
  // aggregate pop pixels via its sum reducer.
  var col = ee.ImageCollection(CONFIG.wpCollection)
    .filterDate(year + '-01-01', (year + 1) + '-01-01');
  var img = col.mosaic()
    .clip(VALID_BOUNDS)
    .setDefaultProjection('EPSG:4326', null, 100);
  return img.set('year', year).set('pop_source_year', year);
}

// -----------------------------------------------------------------------------
// prepAgeBands(wp) -> ee.Image with bands pop_total, age_0, age_1, ..., age_80
// -----------------------------------------------------------------------------
function prepAgeBands(wp) {
  // Build one age-total image per bin: age_x = m_x + f_x.
  // sat-io's WORLDPOP/agesex uses lowercase zero-padded band names
  // (m_00, f_05, etc.), so we zero-pad the bin number when selecting.
  var ageImages = CONFIG.ageBins.map(function(bin) {
    var pad = bin < 10 ? '0' + bin : '' + bin;
    var m = wp.select('m_' + pad);
    var f = wp.select('f_' + pad);
    return m.add(f).rename('age_' + bin);
  });

  // Stack the age-band images into one multi-band image. We can't use
  // ee.ImageCollection(ageImages).sum() because the images have different
  // band names (age_0, age_1, ...) and GEE rejects that as a heterogeneous
  // collection. Instead we cat into a single image and reduce across bands.
  var ageStack = ee.Image.cat(ageImages);

  // pop_total is the per-pixel sum across all age bins. Defining it this way
  // avoids depending on a separate 'population' band that may or may not
  // exist in the sat-io mirror, and it is mathematically identical since the
  // age bins cover 100% of the population by construction.
  var popTotal = ageStack.reduce(ee.Reducer.sum()).rename('pop_total');

  return ee.Image.cat([popTotal, ageStack]);
}

// -----------------------------------------------------------------------------
// computeStatsForYear(boundaries, year, idField, nameField) -> ee.FeatureCollection
// -----------------------------------------------------------------------------
function computeStatsForYear(boundaries, year, idField, nameField) {
  var pm25          = loadPM25(year);
  var wp            = loadWorldPop(year);  // already has setDefaultProjection
  var popSourceYear = ee.Image(wp).get('pop_source_year');
  var popImage      = prepAgeBands(wp);    // pop_total + 20 age bands at 100m

  // Per-pixel pm25 * pop_total forms the numerator of the weighted mean.
  // We do NOT pre-align pop onto the pm25 grid; earlier attempts with
  // reduceResolution + reproject tripped "Unable to transform edge" errors
  // at the poles because GEE evaluated target pixels outside the valid
  // latitude range. Instead we stack pm25 and pop at their native grids and
  // let reduceRegions do all the aggregation via its sum reducer + scale
  // argument. For a SUM reducer, coarser-scale reduceRegions on a finer
  // source image aggregates correctly via GEE's pyramid, so summing the
  // 100m pop surface at scale 1113 gives the true total population in each
  // feature.
  var pm25xPop = pm25.multiply(popImage.select('pop_total'))
    .rename('pm25_x_pop');

  // Include pm25 and a pixel_count band in the sum stack so we can derive
  // the unweighted spatial mean (sum_pm25 / pixel_count) from a single
  // reduceRegions call instead of running a second reduceRegions with a
  // mean reducer + an expensive inner join. This roughly halves compute.
  var pixelCount = pm25.multiply(0).add(1).rename('pixel_count');
  var sumStack = ee.Image.cat([popImage, pm25xPop, pm25.rename('sum_pm25'), pixelCount]);

  // Slim the boundary features down to just feature_id + name AND intersect
  // each geometry with VALID_BOUNDS. The intersection matters for features
  // like Antarctica (in ne_countries) whose native polygons extend to the
  // south pole — without this, reduceRegions tries to sample pixels past
  // -90 deg lat and the EPSG:4326 transform fails at the edge.
  var slim = boundaries.map(function(f) {
    return ee.Feature(f.geometry().intersection(VALID_BOUNDS, 1), {
      feature_id: f.get(idField),
      name:       f.get(nameField)
    });
  });

  // Single sum reducer for everything: population bands, pm25_x_pop,
  // sum_pm25, and pixel_count. Using one reduceRegions instead of two
  // (sum + mean) plus a join cuts compute roughly in half — this was the
  // main cause of GHS_SMOD batch timeouts.
  var summed = sumStack.reduceRegions({
    collection:   slim,
    reducer:      ee.Reducer.sum(),
    crs:          'EPSG:4326',
    crsTransform: GRID_CRS_TRANSFORM,
    tileScale:    CONFIG.tileScale
  });

  // Derive pm25_mean from the summed results: sum_pm25 / pixel_count.
  return summed.map(function(f) {
    var pm25Mean = ee.Number(f.get('sum_pm25'))
      .divide(ee.Number(f.get('pixel_count')));
    return f
      .set('pm25_mean',       pm25Mean)
      .set('year',            year)
      .set('pop_source_year', popSourceYear);
  });
}

// -----------------------------------------------------------------------------
// processBoundarySet(cfg) -> queues Export.table.toDrive tasks
//   One task per year. If cfg.batchSize is set, also splits the boundary
//   FeatureCollection into batches, giving one task per (batch × year).
// -----------------------------------------------------------------------------
function processBoundarySet(cfg) {
  var boundaries = ee.FeatureCollection(cfg.assetId);

  var years = ee.List.sequence(CONFIG.startYear, CONFIG.endYear).getInfo();

  var selectors = ['feature_id', 'name', 'year', 'pop_source_year',
                   'pop_total', 'pm25_x_pop', 'pm25_mean'];
  CONFIG.ageBins.forEach(function(bin) { selectors.push('age_' + bin); });

  // Helper to queue one export task.
  function exportStats(fc, y, suffix) {
    var stats = computeStatsForYear(fc, y, cfg.idField, cfg.nameField);
    var taskName = 'pm25_' + cfg.name + '_' + y + suffix;
    Export.table.toDrive({
      collection:     stats,
      description:    taskName,
      folder:         CONFIG.driveFolder,
      fileNamePrefix: taskName,
      fileFormat:     'CSV',
      selectors:      selectors
    });
  }

  if (cfg.batchSize) {
    // Split into batches to avoid reduceRegions timeouts on large FCs.
    var totalSize = boundaries.size().getInfo();
    var numBatches = Math.ceil(totalSize / cfg.batchSize);
    print(cfg.name + ': ' + totalSize + ' features -> ' +
          numBatches + ' batches of ' + cfg.batchSize);

    for (var b = 0; b < numBatches; b++) {
      var batchList = boundaries.toList(cfg.batchSize, b * cfg.batchSize);
      var batchFC = ee.FeatureCollection(batchList);
      var batchStr = '_' + ('000' + b).slice(-3);  // _000, _001, ...
      years.forEach(function(y) {
        exportStats(batchFC, y, batchStr);
      });
    }
  } else {
    years.forEach(function(y) {
      exportStats(boundaries, y, '');
    });
  }
}

// =============================================================================
// MAIN
// =============================================================================
CONFIG.boundaries.forEach(processBoundarySet);
print('Queued ' + CONFIG.boundaries.length + ' exports to Drive folder "' +
      CONFIG.driveFolder + '". Open the Tasks panel (right sidebar) and click ' +
      'Run on each pm25_* task to start the exports.');
