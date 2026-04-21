/**
 * Backend API client for file uploads and spatial compute.
 * All endpoints are proxied via Vite: /api/* → http://localhost:8000/api/*
 */

const API_BASE = '/api'

/**
 * Upload a geospatial file to the backend.
 *
 * @param {File} file - The file to upload.
 * @param {string} category - One of: "concentration", "population", "boundary".
 * @returns {Promise<object>} The FileUploadOut response from the server.
 */
export async function uploadFile(file, category) {
  const form = new FormData()
  form.append('file', file)
  form.append('category', category)

  const res = await fetch(`${API_BASE}/uploads`, {
    method: 'POST',
    body: form,
  })

  if (!res.ok) {
    const detail = await res.json().catch(() => ({}))
    throw new Error(detail.detail || `Upload failed: ${res.status}`)
  }

  return res.json()
}

/**
 * List uploaded files, optionally filtered by category.
 *
 * @param {string} [category] - Filter by category.
 * @returns {Promise<object[]>}
 */
export async function listUploads(category) {
  const params = category ? `?category=${category}` : ''
  const res = await fetch(`${API_BASE}/uploads${params}`)
  if (!res.ok) throw new Error(`Failed to list uploads: ${res.status}`)
  return res.json()
}

/**
 * Delete an uploaded file.
 *
 * @param {number} fileId - The file upload ID.
 * @returns {Promise<void>}
 */
export async function deleteUpload(fileId) {
  const res = await fetch(`${API_BASE}/uploads/${fileId}`, { method: 'DELETE' })
  if (!res.ok) throw new Error(`Failed to delete upload: ${res.status}`)
}

/**
 * Run a spatially-resolved HIA computation on the backend.
 *
 * @param {object} config - SpatialComputeRequest body.
 * @returns {Promise<object>} SpatialComputeResponse.
 */
/**
 * Fetch concentration data for a pollutant/country/year.
 *
 * @param {string} pollutant - e.g. "pm25"
 * @param {string} country - Country slug, e.g. "mexico"
 * @param {number} year
 * @returns {Promise<object>} GeoJSON FeatureCollection
 */
export async function fetchConcentration(pollutant, country, year) {
  const res = await fetch(`${API_BASE}/data/concentration/${pollutant}/${country}/${year}`)
  if (!res.ok) {
    if (res.status === 404) return null
    throw new Error(`Failed to fetch concentration data: ${res.status}`)
  }
  return res.json()
}

/**
 * Fetch population data for a country/year.
 *
 * @param {string} country
 * @param {number} year
 * @returns {Promise<object|null>} { country, year, units: [...] } or null if 404
 */
export async function fetchPopulation(country, year) {
  const res = await fetch(`${API_BASE}/data/population/${country}/${year}`)
  if (!res.ok) {
    if (res.status === 404) return null
    throw new Error(`Failed to fetch population data: ${res.status}`)
  }
  return res.json()
}

/**
 * Fetch incidence rates for a country/cause/year.
 *
 * @param {string} country
 * @param {string} cause - e.g. "all-cause-mortality"
 * @param {number} year
 * @returns {Promise<object|null>} { country, cause, year, units: [...] } or null if 404
 */
export async function fetchIncidence(country, cause, year) {
  const res = await fetch(`${API_BASE}/data/incidence/${country}/${cause}/${year}`)
  if (!res.ok) {
    if (res.status === 404) return null
    throw new Error(`Failed to fetch incidence data: ${res.status}`)
  }
  return res.json()
}

/**
 * List available built-in datasets.
 *
 * @param {object} [filters] - Optional { pollutant, country, type }
 * @returns {Promise<object>} { datasets: [...] }
 */
export async function fetchDatasets(filters = {}) {
  const params = new URLSearchParams()
  if (filters.pollutant) params.set('pollutant', filters.pollutant)
  if (filters.country) params.set('country', filters.country)
  if (filters.type) params.set('type', filters.type)
  const qs = params.toString()
  const res = await fetch(`${API_BASE}/data/datasets${qs ? `?${qs}` : ''}`)
  if (!res.ok) throw new Error(`Failed to fetch datasets: ${res.status}`)
  return res.json()
}

export async function runSpatialCompute(config) {
  const res = await fetch(`${API_BASE}/compute/spatial`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(config),
  })

  if (!res.ok) {
    const detail = await res.json().catch(() => ({}))
    throw new Error(detail.detail || `Spatial compute failed: ${res.status}`)
  }

  return res.json()
}

/**
 * Fetch ACS 5-year tract demographics for a country/year.
 *
 * @param {string} country - Country slug (e.g. 'us').
 * @param {number} year - ACS 5-year vintage (end year).
 * @param {object} [opts]
 * @param {string} [opts.state] - 2-digit state FIPS filter.
 * @param {string} [opts.county] - 3-digit county FIPS filter (requires state).
 * @param {number} [opts.simplify] - Geometry simplification tolerance in degrees.
 * @returns {Promise<object|null>} GeoJSON FeatureCollection, or null on 404.
 */
export async function fetchDemographics(country, year, opts = {}) {
  const params = new URLSearchParams()
  if (opts.state) params.set('state', opts.state)
  if (opts.county) params.set('county', opts.county)
  if (opts.simplify !== undefined) params.set('simplify', String(opts.simplify))
  const qs = params.toString()
  const res = await fetch(
    `${API_BASE}/data/demographics/${country}/${year}${qs ? `?${qs}` : ''}`,
  )
  if (!res.ok) {
    if (res.status === 404) return null
    throw new Error(`Failed to fetch demographics: ${res.status}`)
  }
  return res.json()
}
