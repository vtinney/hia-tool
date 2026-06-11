import { useEffect, useRef } from 'react'
import mapboxgl from 'mapbox-gl'
import 'mapbox-gl/dist/mapbox-gl.css'

// Color ramp for percentage fields (0.0 – 1.0). 7 stops from light to dark.
const PCT_RAMP = [
  0.0, '#f7fbff',
  0.15, '#deebf7',
  0.30, '#c6dbef',
  0.45, '#9ecae1',
  0.60, '#6baed6',
  0.75, '#3182bd',
  0.90, '#08519c',
]

/**
 * Render a FeatureCollection of tract polygons as a choropleth on a MapBox GL map.
 *
 * @param {object} props
 * @param {object} props.geojson - FeatureCollection with numeric `field` on each feature.
 * @param {string} props.field - Property name to drive choropleth color.
 * @param {string} [props.accessToken] - Mapbox access token. Falls back to VITE_MAPBOX_TOKEN env.
 * @param {[number, number, number, number]} [props.bbox] - Optional fitBounds bbox.
 */
export default function TractChoroplethMap({ geojson, field, accessToken, bbox }) {
  const containerRef = useRef(null)
  const mapRef = useRef(null)

  useEffect(() => {
    if (!containerRef.current) return
    const token = accessToken || import.meta.env.VITE_MAPBOX_TOKEN
    if (!token) {
      console.warn('TractChoroplethMap: no Mapbox token; map will not render')
      return
    }
    mapboxgl.accessToken = token

    const map = new mapboxgl.Map({
      container: containerRef.current,
      style: 'mapbox://styles/mapbox/light-v11',
      center: [-98, 39],
      zoom: 3,
    })
    mapRef.current = map

    map.on('load', () => {
      map.addSource('tracts', { type: 'geojson', data: geojson })
      map.addLayer({
        id: 'tracts-fill',
        type: 'fill',
        source: 'tracts',
        paint: {
          'fill-color': [
            'case',
            ['!=', ['typeof', ['get', field]], 'number'],
            '#eeeeee', // no-data flat fill
            ['interpolate', ['linear'], ['get', field], ...PCT_RAMP],
          ],
          'fill-opacity': 0.75,
        },
      })
      map.addLayer({
        id: 'tracts-line',
        type: 'line',
        source: 'tracts',
        paint: { 'line-color': '#ffffff', 'line-width': 0.3, 'line-opacity': 0.4 },
      })

      if (bbox) {
        map.fitBounds([[bbox[0], bbox[1]], [bbox[2], bbox[3]]], { padding: 24, duration: 0 })
      }
    })

    return () => {
      map.remove()
      mapRef.current = null
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // Update data when geojson changes
  useEffect(() => {
    const map = mapRef.current
    if (!map || !map.getSource || !map.getSource('tracts')) return
    map.getSource('tracts').setData(geojson)
  }, [geojson])

  // Update paint expression when field changes
  useEffect(() => {
    const map = mapRef.current
    if (!map || !map.getLayer || !map.getLayer('tracts-fill')) return
    map.setPaintProperty('tracts-fill', 'fill-color', [
      'case',
      ['!=', ['typeof', ['get', field]], 'number'],
      '#eeeeee',
      ['interpolate', ['linear'], ['get', field], ...PCT_RAMP],
    ])
  }, [field])

  return <div ref={containerRef} className="w-full h-[480px] rounded-xl overflow-hidden" />
}
