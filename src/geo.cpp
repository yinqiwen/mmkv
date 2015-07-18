/*
 *Copyright (c) 2015-2015, yinqiwen <yinqiwen@gmail.com>
 *All rights reserved.
 *
 *Redistribution and use in source and binary forms, with or without
 *modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 *THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
 *BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 *THE POSSIBILITY OF SUCH DAMAGE.
 */
#include "lock_guard.hpp"
#include "mmkv_impl.hpp"
#include "utils.hpp"
#include "geohash.h"
#include <algorithm>
#include <vector>
#include <deque>
#include <set>
#include <math.h>

#define GEO_WGS84_TYPE 1
#define GEO_MERCATOR_TYPE 2

#define D_R (M_PI / 180.0)
#define R_D (180.0 / M_PI)
#define R_MAJOR 6378137.0
#define R_MINOR 6356752.3142
#define RATIO (R_MINOR/R_MAJOR)
#define ECCENT (sqrt(1.0 - (RATIO * RATIO)))
#define COM (0.5 * ECCENT)

namespace mmkv
{
    /// @brief The usual PI/180 constant
    static const long double DEG_TO_RAD = 0.017453292519943295769236907684886;
    /// @brief Earth's quatratic mean radius for WGS-84
    static const long double EARTH_RADIUS_IN_METERS = 6372797.560856;

    static const long double MERCATOR_MAX = 20037726.37;
    static const long double MERCATOR_MIN = -20037726.37;

    struct GeoHashBitsComparator
    {
            bool operator()(const GeoHashBits& a, const GeoHashBits& b) const
            {
                if (a.step < b.step)
                {
                    return true;
                }
                if (a.step > b.step)
                {
                    return false;
                }
                return a.bits < b.bits;
            }
    };
    typedef std::set<GeoHashBits, GeoHashBitsComparator> GeoHashBitsSet;

    static inline long double deg_rad(long double ang)
    {
        return ang * D_R;
    }

    static inline long double mercator_y(long double lat)
    {
        if (lat > 90 || lat < -90)
        {
            return lat;
        }
        lat = fmin(89.5, fmax(lat, -89.5));
        long double phi = deg_rad(lat);
        long double sinphi = sin(phi);
        long double con = ECCENT * sinphi;
        con = pow((1.0 - con) / (1.0 + con), COM);
        long double ts = tan(0.5 * (M_PI * 0.5 - phi)) / con;
        return 0 - R_MAJOR * log(ts);
    }

    static inline long double mercator_x(long double lon)
    {
        if (lon > 180 || lon < -180)
        {
            return lon;
        }
        return R_MAJOR * deg_rad(lon);
    }
    static inline long double rad_deg(long double ang)
    {
        return ang * R_D;
    }
    static inline long double merc_lon(long double x)
    {
        return rad_deg(x) / R_MAJOR;
    }

    static inline long double merc_lat(long double y)
    {
        long double ts = exp(-y / R_MAJOR);
        long double phi = M_PI_2 - 2 * atan(ts);
        long double dphi = 1.0;
        int i;
        for (i = 0; fabs(dphi) > 0.000000001 && i < 15; i++)
        {
            long double con = ECCENT * sin(phi);
            dphi = M_PI_2 - 2 * atan(ts * pow((1.0 - con) / (1.0 + con), COM)) - phi;
            phi += dphi;
        }
        return rad_deg(phi);
    }
    static inline int get_coord_type(const Data& coord_type)
    {
        if (!strcasecmp(coord_type.Value(), "wgs84"))
        {
            return GEO_WGS84_TYPE;
        }
        else if (!strcasecmp(coord_type.Value(), "mercator"))
        {
            return GEO_MERCATOR_TYPE;
        }
        else
        {
            return -1;
        }
    }
    static inline int get_coord_range(int coord_type, GeoHashRange& lat_range, GeoHashRange& lon_range)
    {
        switch (coord_type)
        {
            case GEO_WGS84_TYPE:
            {
                lat_range.max = 85.05113;   //
                lat_range.min = -85.05113;  //
                lon_range.max = 180.0;
                lon_range.min = -180.0;
                break;
            }
            case GEO_MERCATOR_TYPE:
            {
                lat_range.max = 20037726.37;
                lat_range.min = -20037726.37;
                lon_range.max = 20037726.37;
                lon_range.min = -20037726.37;
                break;
            }
            default:
            {
                return -1;
            }
        }
        return 0;
    }

    static inline uint8_t estimate_geohash_steps_by_radius(long double range_meters)
    {
        uint8_t step = 1;
        long double v = range_meters;
        while (v < MERCATOR_MAX)
        {
            v *= 2;
            step++;
        }
        step--;
        return step;
    }

    static bool get_xy_by_hash(int coord_type, uint64_t hash, long double& x, long double& y)
    {
        GeoHashRange lat_range, lon_range;
        get_coord_range(coord_type, lat_range, lon_range);
        GeoHashBits hashbits;
        hashbits.bits = hash;
        hashbits.step = 30;
        GeoHashArea area;
        if (0 == geohash_fast_decode(lat_range, lon_range, hashbits, &area))
        {
            y = (area.latitude.min + area.latitude.max) / 2;
            x = (area.longitude.min + area.longitude.max) / 2;
            return true;
        }
        else
        {
            return false;
        }
    }

    static inline int get_areas_by_radius(int coord_type, long double latitude, long double longitude,
            long double radius_meters, GeoHashBitsSet& results)
    {
        GeoHashRange lat_range, lon_range;
        get_coord_range(coord_type, lat_range, lon_range);
        long double delta_longitude = radius_meters;
        long double delta_latitude = radius_meters;
        if (coord_type == GEO_WGS84_TYPE)
        {
            delta_latitude = radius_meters / (111320.0 * cos(latitude));
            delta_longitude = radius_meters / 110540.0;
        }

        long double min_lat = latitude - delta_latitude;
        long double max_lat = latitude + delta_latitude;
        long double min_lon = longitude - delta_longitude;
        long double max_lon = longitude + delta_longitude;

        int steps = estimate_geohash_steps_by_radius(radius_meters);
        GeoHashBits hash;
        geohash_fast_encode(lat_range, lat_range, latitude, longitude, steps, &hash);

        GeoHashArea area;
        geohash_fast_decode(lat_range, lat_range, hash, &area);
        results.insert(hash);

        long double range_lon = (area.longitude.max - area.longitude.min) / 2;
        long double range_lat = (area.latitude.max - area.latitude.min) / 2;

        GeoHashNeighbors neighbors;

        bool split_east = false;
        bool split_west = false;
        bool split_north = false;
        bool split_south = false;
        if (max_lon > area.longitude.max)
        {
            geohash_get_neighbor(hash, GEOHASH_EAST, &(neighbors.east));
            if (area.longitude.max + range_lon > max_lon)
            {
                results.insert(geohash_next_leftbottom(neighbors.east));
                results.insert(geohash_next_lefttop(neighbors.east));
                split_east = true;
            }
            else
            {
                results.insert(neighbors.east);
            }
        }
        if (min_lon < area.longitude.min)
        {
            geohash_get_neighbor(hash, GEOHASH_WEST, &neighbors.west);
            if (area.longitude.min - range_lon < min_lon)
            {
                results.insert(geohash_next_rightbottom(neighbors.west));
                results.insert(geohash_next_righttop(neighbors.west));
                split_west = true;
            }
            else
            {
                results.insert(neighbors.west);
            }
        }
        if (max_lat > area.latitude.max)
        {
            geohash_get_neighbor(hash, GEOHASH_NORTH, &neighbors.north);
            if (area.latitude.max + range_lat > max_lat)
            {
                results.insert(geohash_next_rightbottom(neighbors.north));
                results.insert(geohash_next_leftbottom(neighbors.north));
                split_north = true;
            }
            else
            {
                results.insert(neighbors.north);
            }
        }
        if (min_lat < area.latitude.min)
        {
            geohash_get_neighbor(hash, GEOHASH_SOUTH, &neighbors.south);

            if (area.latitude.min - range_lat < min_lat)
            {
                results.insert(geohash_next_righttop(neighbors.south));
                results.insert(geohash_next_lefttop(neighbors.south));
                split_south = true;
            }
            else
            {
                results.insert(neighbors.south);
            }
        }

        if (max_lon > area.longitude.max && max_lat > area.latitude.max)
        {

            geohash_get_neighbor(hash, GEOHASH_NORT_EAST, &neighbors.north_east);
            if (split_north && split_east)
            {
                results.insert(geohash_next_leftbottom(neighbors.north_east));
            }
            else if (split_north)
            {
                results.insert(geohash_next_rightbottom(neighbors.north_east));
                results.insert(geohash_next_leftbottom(neighbors.north_east));
            }
            else if (split_east)
            {
                results.insert(geohash_next_leftbottom(neighbors.north_east));
                results.insert(geohash_next_lefttop(neighbors.north_east));
            }
            else
            {
                results.insert(neighbors.north_east);
            }
        }
        if (max_lon > area.longitude.max && min_lat < area.latitude.min)
        {
            geohash_get_neighbor(hash, GEOHASH_SOUTH_EAST, &neighbors.south_east);
            if (split_south && split_east)
            {
                results.insert(geohash_next_lefttop(neighbors.south_east));
            }
            else if (split_south)
            {
                results.insert(geohash_next_righttop(neighbors.south_east));
                results.insert(geohash_next_lefttop(neighbors.south_east));
            }
            else if (split_east)
            {
                results.insert(geohash_next_leftbottom(neighbors.south_east));
                results.insert(geohash_next_lefttop(neighbors.south_east));
            }
            else
            {
                results.insert(neighbors.south_east);
            }
        }
        if (min_lon < area.longitude.min && max_lat > area.latitude.max)
        {
            geohash_get_neighbor(hash, GEOHASH_NORT_WEST, &neighbors.north_west);
            if (split_north && split_west)
            {
                results.insert(geohash_next_rightbottom(neighbors.north_west));
            }
            else if (split_north)
            {
                results.insert(geohash_next_rightbottom(neighbors.north_west));
                results.insert(geohash_next_leftbottom(neighbors.north_west));
            }
            else if (split_west)
            {
                results.insert(geohash_next_rightbottom(neighbors.north_west));
                results.insert(geohash_next_righttop(neighbors.north_west));
            }
            else
            {
                results.insert(neighbors.north_west);
            }
        }
        if (min_lon < area.longitude.min && min_lat < area.latitude.min)
        {
            geohash_get_neighbor(hash, GEOHASH_SOUTH_WEST, &neighbors.south_west);
            if (split_south && split_west)
            {
                results.insert(geohash_next_righttop(neighbors.south_west));
            }
            else if (split_south)
            {
                results.insert(geohash_next_righttop(neighbors.south_west));
                results.insert(geohash_next_lefttop(neighbors.south_west));
            }
            else if (split_west)
            {
                results.insert(geohash_next_rightbottom(neighbors.south_west));
                results.insert(geohash_next_righttop(neighbors.south_west));
            }
            else
            {
                results.insert(neighbors.south_west);
            }
        }
        return 0;
    }

    static bool verify_distance_if_in_radius(long double x1, long double y1, long double x2, long double y2,
            long double radius, long double& distance_square, long double accurace)
    {
        long double xx = (x1 - x2) * (x1 - x2);
        long double yy = (y1 - y2) * (y1 - y2);
        long double dd = xx + yy;
        long double rr = (radius + accurace) * (radius + accurace);
        if (dd > rr)
        {
            return false;
        }
        distance_square = dd;
        return true;
    }

    static inline uint64_t allign60bits(const GeoHashBits& hash)
    {
        uint64_t bits = hash.bits;
        bits <<= (60 - hash.step * 2);
        return bits;
    }

    int MMKVImpl::GeoAdd(DBID db, const Data& key, const Data& coord_type_str, const GeoPointArray& points)
    {
        if (m_readonly)
        {
            return ERR_PERMISSION_DENIED;
        }
        int coord_type = get_coord_type(coord_type_str);
        if (coord_type < 0)
        {
            return ERR_INVALID_COORD_TYPE;
        }
        GeoHashRange lat_range, lon_range;
        get_coord_range(GEO_MERCATOR_TYPE, lat_range, lon_range);
        GeoPointArray mercator_points;
        GeoPointArray::const_iterator it = points.begin();
        while (it != points.end())
        {
            const GeoPoint& point = *it;
            GeoPoint mercator_point = point;
            /*
             * Always to store mercator coordinates since it's easy&fast to compare distance in 'geosearch'
             */
            if (coord_type == GEO_WGS84_TYPE)
            {
                mercator_point.x = mercator_x(point.x);
                mercator_point.y = mercator_y(point.y);
            }
            if (mercator_point.x < lon_range.min || mercator_point.x > lon_range.max || mercator_point.y < lat_range.min
                    || mercator_point.y > lat_range.max)
            {
                return ERR_INVALID_COORD_VALUE;
            }
            mercator_points.push_back(mercator_point);
            it++;
        }

        RWLockGuard<MemorySegmentManager, WRITE_LOCK> keylock_guard(m_segment);
        EnsureWritableValueSpace();
        Allocator<char> allocator = m_segment.ValueAllocator<char>();
        int err;
        ZSet* zset = GetObject<ZSet>(db, key, V_TYPE_ZSET, true, err)(allocator);
        if (0 != err)
        {
            return err;
        }
        int inserted = 0;
        for (size_t i = 0; i < mercator_points.size(); i++)
        {
            const GeoPoint& point = mercator_points[i];
            GeoHashBits hash;
            geohash_fast_encode(lat_range, lon_range, point.y, point.x, 30, &hash);
            long double score = (long double) hash.bits;

            Object tmpk(point.value, true);
            std::pair<StringDoubleTable::iterator, bool> ret = zset->scores.insert(
                    StringDoubleTable::value_type(tmpk, score));
            if (ret.second)
            {
                ScoreValue fv;
                AssignScoreValue(fv, score, point.value);
                const_cast<Object&>(ret.first->first) = fv.value;
                zset->set.insert(fv);
                inserted++;
            }
            else
            {
                if (ret.first->second == score)
                {
                    continue;
                }
                if (0 == UpdateZSetScore(*zset, tmpk, ret.first->second, score))
                {
                    ret.first->second = score;
                }
                else
                {
                    ABORT("Can not replace old zset element score.");
                }
            }
        }
        return inserted;
    }

    struct GeoPointResult
    {
            long double x, y, distance;
            Object value;
            GeoPointResult() :
                    x(0), y(0), distance(0)
            {
            }
    };

    static bool less_by_distance(const GeoPointResult& v1, const GeoPointResult& v2)
    {
        return v1.distance < v2.distance;
    }

    static bool great_by_distance(const GeoPointResult& v1, const GeoPointResult& v2)
    {
        return v1.distance > v2.distance;
    }

    int MMKVImpl::GeoSearch(DBID db, const Data& key, const GeoSearchOptions& options, const StringArrayResult& results)
    {
        int coord_type = GEO_MERCATOR_TYPE;
        long double x = options.by_x, y = options.by_y;
        if (!options.by_member.empty())
        {
            long double hash;
            int err = ZScore(db, key, options.by_member, hash);
            if (0 != err)
            {
                return err;
            }
            get_xy_by_hash(GEO_MERCATOR_TYPE, (uint64_t) hash, x, y);
        }
        else
        {
            coord_type = get_coord_type(options.coord_type);
            if(coord_type < 0)
            {
                return ERR_INVALID_COORD_TYPE;
            }
            if (coord_type == GEO_WGS84_TYPE)
            {
                x = mercator_x(x);
                y = mercator_y(y);
            }
        }
        int err = 0;
        uint32_t min_radius = 75; //magic number
        if (options.asc && options.limit > 0 && options.offset == 0 && options.radius > min_radius)
        {
            GeoSearchOptions current_options = options;
            uint32_t old_radius = options.radius;
            current_options.radius = min_radius;
            do
            {
                int point_num = GeoSearchWithMinLimit(db, key, current_options, coord_type, x, y, options.limit, results);
                if (point_num < 0)
                {
                    return point_num;
                }
                if (current_options.radius >= old_radius)
                {
                    break;
                }
                if (point_num > 0)
                {
                    if (point_num >= current_options.limit)
                    {
                        break;
                    }
                    current_options.radius *= (uint32_t) (sqrt((current_options.limit / point_num) + 1));
                }
                else
                {
                    current_options.radius *= 8;
                }
                if (current_options.radius > old_radius)
                {
                    current_options.radius = old_radius;
                }
            } while (current_options.radius <= old_radius);
        }
        else
        {
            err = GeoSearchWithMinLimit(db, key, options, coord_type, x, y, -1, results);
        }
        return err >= 0 ? 0 : err;
    }

    struct GeoHashRangeSepc
    {
            uint64_t start;
            uint64_t stop;
    };
    int MMKVImpl::GeoSearchWithMinLimit(DBID db, const Data& key, const GeoSearchOptions& options, int coord_type,
            long double x, long double y, int min_limit, const StringArrayResult& results)
    {
        GeoHashRange lat_range, lon_range;
        get_coord_range(GEO_MERCATOR_TYPE, lat_range, lon_range);
        GeoHashBitsSet ress;
        get_areas_by_radius(GEO_MERCATOR_TYPE, y, x, options.radius, ress);
        GeoHashBitsSet::iterator rit = ress.begin();
        typedef std::map<uint64_t, uint64_t> HashRangeMap;
        HashRangeMap tmp;
        while (rit != ress.end())
        {
            const GeoHashBits& hash = *rit;
            GeoHashBits next = hash;
            next.bits++;
            tmp[allign60bits(hash)] = allign60bits(next);
            rit++;
        }
        std::vector<GeoHashRangeSepc> range_array;
        HashRangeMap::iterator tit = tmp.begin();
        HashRangeMap::iterator nit = tmp.begin();
        nit++;
        while (tit != tmp.end())
        {
            GeoHashRangeSepc range;
            range.start = tit->first;
            range.stop = tit->second;
            while (nit != tmp.end() && nit->first == range.stop)
            {
                range.stop = nit->second;
                nit++;
                tit++;
            }
            range_array.push_back(range);
            nit++;
            tit++;
        }
        DEBUG_LOG("After areas merging, reduce searching area size from %u to %u", ress.size(), range_array.size());

        std::vector<GeoPointResult> points;
        RWLockGuard<MemorySegmentManager, READ_LOCK> keylock_guard(m_segment);
        int err;
        ZSet* zset = GetObject<ZSet>(db, key, V_TYPE_ZSET, false, err)();
        if (IS_NOT_EXISTS(err))
        {
            return 0;
        }
        if (0 != err)
        {
            return err;
        }
        MMKVTable* kv = GetMMKVTable(db, false);
        std::vector<GeoHashRangeSepc>::iterator range_it = range_array.begin();
        SortedSet::iterator last_it = zset->set.end();
        while (range_it != range_array.end())
        {
            ScoreValue min_sv;
            min_sv.score = range_it->start;
            SortedSet::iterator min_it = zset->set.lower_bound(min_sv);
            while (min_it != zset->set.end())
            {
                ScoreValue& sv = *min_it;
                if ((uint64_t) sv.score > range_it->stop)
                {
                    break;
                }
                GeoPointResult point;
                point.value = sv.value;
                get_xy_by_hash(GEO_MERCATOR_TYPE, (uint64_t) sv.score, point.x, point.y);
                if (verify_distance_if_in_radius(x, y, point.x, point.y, options.radius, point.distance, 0.2))
                {
                    bool valid_value = true;
                    if (!options.includes.empty())
                    {
                        GeoSearchOptions::PatternMap::const_iterator sit = options.includes.begin();
                        while (sit != options.includes.end())
                        {
                            if (!MatchValueByPattern(kv, sit->first, sit->second, point.value))
                            {
                                valid_value = false;
                                break;
                            }
                            else
                            {
                                valid_value = true;
                            }
                            sit++;
                        }
                    }
                    if (valid_value && !options.excludes.empty())
                    {
                        GeoSearchOptions::PatternMap::const_iterator sit = options.excludes.begin();
                        while (sit != options.excludes.end())
                        {
                            if (MatchValueByPattern(kv, sit->first, sit->second, point.value))
                            {
                                valid_value = false;
                                break;
                            }
                            else
                            {
                                valid_value = true;
                            }
                            sit++;
                        }
                    }
                    if (valid_value)
                    {
                        points.push_back(point);
                    }
                }
                min_it++;
            }
            range_it++;
        }

        if (min_limit > 0 && points.size() < (size_t) min_limit)
        {
            return points.size();
        }

        std::sort(points.begin(), points.end(), options.asc ? less_by_distance : great_by_distance);
        size_t offset = 0;
        if (options.offset > 0)
        {
            offset = options.offset;
        }
        size_t limit = points.size();
        if (options.limit > 0)
        {
            limit = offset + options.limit;
        }
        for (size_t i = offset; i < limit; i++)
        {
            GeoPointResult& point = points[i];
            point.value.ToString(results.Get());
            GeoSearchOptions::PatternArray::const_iterator ait = options.get_patterns.begin();
            while (ait != options.get_patterns.end())
            {
                const std::string& pattern = *ait;
                if (!strcasecmp(GEO_SEARCH_WITH_DISTANCES, pattern.c_str()))
                {
                    char dbuf[256];
                    snprintf(dbuf, sizeof(dbuf), "%.2f", sqrt(point.distance));
                    std::string& ss = results.Get();
                    ss = dbuf;
                }
                else if (!strcasecmp(GEO_SEARCH_WITH_COORDINATES, pattern.c_str()))
                {
                    if (coord_type == GEO_WGS84_TYPE)
                    {
                        point.x = merc_lon(point.x);
                        point.y = merc_lat(point.y);
                    }
                    char dbuf[256];
                    snprintf(dbuf, sizeof(dbuf), "%.2Lf", point.x);
                    std::string& xstr = results.Get();
                    xstr = dbuf;
                    snprintf(dbuf, sizeof(dbuf), "%.2Lf", point.y);
                    std::string& ystr = results.Get();
                    ystr = dbuf;
                }
                else
                {
                    int err = GetValueByPattern(kv, pattern, point.value, point.value);
                    if (err < 0)
                    {
                        WARN_LOG("Failed to get value by pattern for:%s in geosearch", pattern.c_str());
                        results.Get(); //empty value
                    }
                    else
                    {
                        point.value.ToString(results.Get());
                    }
                }
                ait++;
            }
        }
        return limit - offset;
    }
}

