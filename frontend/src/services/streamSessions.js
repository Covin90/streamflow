/**
 * API service for Stream Monitoring Sessions
 */
import { api } from './api';

export const streamSessionsAPI = {
  /**
   * Get all stream monitoring sessions
   * @param {string} status - Optional filter by status ('active' or undefined for all)
   * @returns {Promise} Array of sessions
   */
  getSessions: (status = null) => {
    const params = status ? { status } : {};
    return api.get('/stream-sessions', { params });
  },

  /**
   * Get a specific session with details
   * @param {string} sessionId - Session ID
   * @returns {Promise} Session details including streams
   */
  getSession: (sessionId, sinceTimestamp = null) => {
    const params = sinceTimestamp ? { since_timestamp: sinceTimestamp } : {};
    return api.get(`/stream-sessions/${sessionId}`, { params });
  },

  /**
   * Create a new stream monitoring session
   * @param {object} sessionData - Session configuration
   * @returns {Promise} Created session info
   */
  createSession: (sessionData) => {
    return api.post('/stream-sessions', sessionData);
  },

  /**
   * Create and start monitoring sessions for all channels in a group
   * @param {object} sessionData - Session configuration with group_id
   * @returns {Promise} Created sessions info
   */
  createGroupSession: (sessionData) => {
    return api.post('/stream-sessions/group/start', sessionData);
  },

  /**
   * Start monitoring for a session
   * @param {string} sessionId - Session ID
   * @returns {Promise}
   */
  startSession: (sessionId) => {
    return api.post(`/stream-sessions/${sessionId}/start`);
  },

  /**
   * Stop monitoring for a session
   * @param {string} sessionId - Session ID
   * @returns {Promise}
   */
  stopSession: (sessionId) => {
    return api.post(`/stream-sessions/${sessionId}/stop`);
  },

  /**
   * Stop multiple sessions at once
   * @param {string[]} sessionIds - Array of Session IDs
   * @returns {Promise}
   */
  batchStopSessions: (sessionIds) => {
    return api.post('/stream-sessions/batch/stop', { session_ids: sessionIds });
  },

  /**
   * Delete multiple sessions at once
   * @param {string[]} sessionIds - Array of Session IDs
   * @returns {Promise}
   */
  batchDeleteSessions: (sessionIds) => {
    return api.post('/stream-sessions/batch/delete', { session_ids: sessionIds });
  },

  /**
   * Delete a session
   * @param {string} sessionId - Session ID
   * @returns {Promise}
   */
  deleteSession: (sessionId) => {
    return api.delete(`/stream-sessions/${sessionId}`);
  },

  /**
   * Get metrics history for a stream
   * @param {string} sessionId - Session ID
   * @param {number} streamId - Stream ID
   * @param {number} sinceTimestamp - Optional unix timestamp to filter metrics
   * @returns {Promise} Metrics data
   */
  getStreamMetrics: (sessionId, streamId, sinceTimestamp = null) => {
    const params = sinceTimestamp ? { since_timestamp: sinceTimestamp } : {};
    return api.get(`/stream-sessions/${sessionId}/streams/${streamId}/metrics`, { params });
  },

  /**
   * Get screenshot URL for a stream
   * @param {number} streamId - Stream ID
   * @returns {string} Screenshot URL
   */
  getScreenshotUrl: (streamId) => {
    return `/api/data/screenshots/${streamId}.jpg?t=${Date.now()}`;
  },

  /**
   * Get alive screenshots for a session
   * @param {string} sessionId - Session ID
   * @returns {Promise} Array of alive stream screenshots
   */
  getAliveScreenshots: (sessionId) => {
    return api.get(`/stream-sessions/${sessionId}/alive-screenshots`);
  },

  /**
   * Manually quarantine a stream in a session
   * @param {string} sessionId - Session ID
   * @param {number} streamId - Stream ID
   * @returns {Promise}
   */
  quarantineStream: (sessionId, streamId) => {
    return api.post(`/stream-sessions/${sessionId}/streams/${streamId}/quarantine`);
  },

  /**
   * Manually revive a quarantined stream in a session
   * @param {string} sessionId - Session ID
   * @param {number} streamId - Stream ID
   * @returns {Promise}
   */
  reviveStream: (sessionId, streamId) => {
    return api.post(`/stream-sessions/${sessionId}/streams/${streamId}/revive`);
  },

  /**
   * Get current proxy status (which streams are being played)
   * @returns {Promise} Proxy status data
   */
  getProxyStatus: () => {
    return api.get('/proxy/status');
  },

  /**
   * Get list of stream IDs currently being played
   * @returns {Promise} Array of playing stream IDs
   */
  getPlayingStreams: () => {
    return api.get('/proxy/playing-streams');
  },

  /**
   * Get stream viewer URL for live playback
   * Uses the local UDP proxy to avoid consuming provider quota
   * @param {number} streamId - Stream ID
   * @returns {Promise} Stream URL for viewing
   */
  getStreamViewerUrl: (streamId) => {
    // Return the proxy URL directly - this uses FFmpeg's existing connection
    // via local UDP (30000+stream_id), not a new provider connection
    return Promise.resolve({
      data: {
        success: true,
        stream_url: `/api/stream/proxy/${streamId}`,
        stream_id: streamId,
        using_proxy: true,
      }
    });
  },
};
