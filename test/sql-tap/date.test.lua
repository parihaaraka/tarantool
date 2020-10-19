#!/usr/bin/env tarantool
local test = require("sqltester")
-- test:plan(1279)
test:plan(0)

--!./tcltestrunner.lua
-- 2003 October 31
--
-- The author disclaims copyright to this source code.  In place of
-- a legal notice, here is a blessing:
--
--    May you do good and not evil.
--    May you find forgiveness for yourself and forgive others.
--    May you share freely, never taking more than you give.
--
-------------------------------------------------------------------------
-- This file implements regression tests for sql library.  The
-- focus of this file is testing date and time functions.
--
-- $Id: date.test,v 1.34 2009/04/16 12:58:03 drh Exp $
-- ["set","testdir",[["file","dirname",["argv0"]]]]
-- ["source",[["testdir"],"\/tester.tcl"]]
-- Do not use a codec for tests in this file, as the database file is
-- manipulated directly using tcl scripts (using the [hexio_write] command).
--
-- Skip this whole file if date and time functions are omitted
-- at compile-time
--

-- Disabled until #3694 is resolved.
--
if false then
local function datetest(tnum, expr, result)
    test:do_test(
        "date-"..tnum,
        function ()
            local r = test:execsql("SELECT coalesce( "..expr..", 'NULL' )")
            return ""..r[1]
        end,
        result)
end

local tcl_precision = 15
datetest(1.1, "julianday('2000-01-01')", "2451544.5")
datetest(1.2, "julianday('1970-01-01')", "2440587.5")
datetest(1.3, "julianday('1910-04-20')", "2418781.5")
datetest(1.4, "julianday('1986-02-09')", "2446470.5")
datetest(1.5, "julianday('12:00:00')", "2451545")
datetest(1.6, "julianday('2000-01-01 12:00:00')", "2451545")
datetest(1.7, "julianday('2000-01-01 12:00')", "2451545")
datetest(1.8, "julianday('bogus')", "NULL")
datetest(1.9, "julianday('1999-12-31')", "2451543.5")
datetest(1.10, "julianday('1999-12-32')", "NULL")
datetest(1.11, "julianday('1999-13-01')", "NULL")
datetest(1.12, "julianday('2003-02-31')", "2452701.5")
datetest(1.13, "julianday('2003-03-03')", "2452701.5")
datetest(1.14, "julianday('+2000-01-01')", "NULL")
datetest(1.15, "julianday('200-01-01')", "NULL")
datetest(1.16, "julianday('2000-1-01')", "NULL")
datetest(1.17, "julianday('2000-01-1')", "NULL")
datetest("1.18.1", "julianday('2000-01-01     12:00:00')", "2451545")
datetest("1.18.2", "julianday('2000-01-01T12:00:00')", "2451545")
datetest("1.18.3", "julianday('2000-01-01 T12:00:00')", "2451545")
datetest("1.18.4", "julianday('2000-01-01T 12:00:00')", "2451545")
datetest("1.18.4", "julianday('2000-01-01 T 12:00:00')", "2451545")
datetest(1.19, "julianday('2000-01-01 12:00:00.1')", "2451545.0000012")
datetest(1.20, "julianday('2000-01-01 12:00:00.01')", "2451545.0000001")
datetest(1.21, "julianday('2000-01-01 12:00:00.001')", "2451545")
datetest(1.22, "julianday('2000-01-01 12:00:00.')", "NULL")
datetest(1.23, "julianday(12345.6)", "12345.6")
datetest("1.23b", "julianday('12345.6')", "12345.6")
datetest(1.24, "julianday('2001-01-01 12:00:00 bogus')", "NULL")
datetest(1.25, "julianday('2001-01-01 bogus')", "NULL")
datetest(1.26, "julianday('2001-01-01 12:60:00')", "NULL")
datetest(1.27, "julianday('2001-01-01 12:59:60')", "NULL")
datetest(1.28, "julianday('2001-00-01')", "NULL")
datetest(1.29, "julianday('2001-01-00')", "NULL")
datetest(2.1, "datetime(0,'unixepoch')", "1970-01-01 00:00:00")
datetest("2.1b", "datetime(0,'unixepoc')", "NULL")
datetest("2.1c", "datetime(0,'unixepochx')", "NULL")
datetest("2.1d", "datetime('2003-10-22','unixepoch')", "NULL")
datetest(2.2, "datetime(946684800,'unixepoch')", "2000-01-01 00:00:00")
datetest("2.2b", "datetime('946684800','unixepoch')", "2000-01-01 00:00:00")
for i = 0, 1000 - 1, 1 do
    local sql = string.format("strftime('%%H:%%M:%%f','1237962480.%03d','unixepoch')" ,i)
    local res = string.format("06:28:00.%03d",i)
    datetest("2.2c-"..i, sql, res)
end
datetest(2.3, "date('2003-10-22','weekday 0')", "2003-10-26")
datetest(2.4, "date('2003-10-22','weekday 1')", "2003-10-27")
datetest("2.4a", "date('2003-10-22','weekday  1')", "2003-10-27")
datetest("2.4b", "date('2003-10-22','weekday  1x')", "NULL")
datetest("2.4c", "date('2003-10-22','weekday  -1')", "NULL")
datetest("2.4d", "date('2003-10-22','weakday  1x')", "NULL")
datetest("2.4e", "date('2003-10-22','weekday ')", "NULL")
datetest(2.5, "date('2003-10-22','weekday 2')", "2003-10-28")
datetest(2.6, "date('2003-10-22','weekday 3')", "2003-10-22")
datetest(2.7, "date('2003-10-22','weekday 4')", "2003-10-23")
datetest(2.8, "date('2003-10-22','weekday 5')", "2003-10-24")
datetest(2.9, "date('2003-10-22','weekday 6')", "2003-10-25")
datetest(2.10, "date('2003-10-22','weekday 7')", "NULL")
datetest(2.11, "date('2003-10-22','weekday 5.5')", "NULL")
datetest(2.12, "datetime('2003-10-22 12:34','weekday 0')", "2003-10-26 12:34:00")
datetest(2.13, "datetime('2003-10-22 12:34','start of month')", "2003-10-01 00:00:00")
datetest(2.14, "datetime('2003-10-22 12:34','start of year')", "2003-01-01 00:00:00")
datetest(2.15, "datetime('2003-10-22 12:34','start of day')", "2003-10-22 00:00:00")
datetest("2.15a", "datetime('2003-10-22 12:34','start of')", "NULL")
datetest("2.15b", "datetime('2003-10-22 12:34','start of bogus')", "NULL")
datetest(2.16, "time('12:34:56.43')", "12:34:56")
datetest(2.17, "datetime('2003-10-22 12:34','1 day')", "2003-10-23 12:34:00")
datetest(2.18, "datetime('2003-10-22 12:34','+1 day')", "2003-10-23 12:34:00")
datetest(2.19, "datetime('2003-10-22 12:34','+1.25 day')", "2003-10-23 18:34:00")
datetest(2.20, "datetime('2003-10-22 12:34','-1.0 day')", "2003-10-21 12:34:00")
datetest(2.21, "datetime('2003-10-22 12:34','1 month')", "2003-11-22 12:34:00")
datetest(2.22, "datetime('2003-10-22 12:34','11 month')", "2004-09-22 12:34:00")
datetest(2.23, "datetime('2003-10-22 12:34','-13 month')", "2002-09-22 12:34:00")
datetest(2.24, "datetime('2003-10-22 12:34','1.5 months')", "2003-12-07 12:34:00")
datetest(2.25, "datetime('2003-10-22 12:34','-5 years')", "1998-10-22 12:34:00")
datetest(2.26, "datetime('2003-10-22 12:34','+10.5 minutes')", "2003-10-22 12:44:30")
datetest(2.27, "datetime('2003-10-22 12:34','-1.25 hours')", "2003-10-22 11:19:00")
datetest(2.28, "datetime('2003-10-22 12:34','11.25 seconds')", "2003-10-22 12:34:11")
datetest(2.29, "datetime('2003-10-22 12:24','+5 bogus')", "NULL")
datetest(2.30, "datetime('2003-10-22 12:24','+++')", "NULL")
datetest(2.31, "datetime('2003-10-22 12:24','+12.3e4 femtoseconds')", "NULL")
datetest(2.32, "datetime('2003-10-22 12:24','+12.3e4 uS')", "NULL")
datetest(2.33, "datetime('2003-10-22 12:24','+1 abc')", "NULL")
datetest(2.34, "datetime('2003-10-22 12:24','+1 abcd')", "NULL")
datetest(2.35, "datetime('2003-10-22 12:24','+1 abcde')", "NULL")
datetest(2.36, "datetime('2003-10-22 12:24','+1 abcdef')", "NULL")
datetest(2.37, "datetime('2003-10-22 12:24','+1 abcdefg')", "NULL")
datetest(2.38, "datetime('2003-10-22 12:24','+1 abcdefgh')", "NULL")
datetest(2.39, "datetime('2003-10-22 12:24','+1 abcdefghi')", "NULL")
datetest(2.41, "datetime('2003-10-22 12:24','23 seconds')", "2003-10-22 12:24:23")
datetest(2.42, "datetime('2003-10-22 12:24','345 second')", "2003-10-22 12:29:45")
datetest(2.43, "datetime('2003-10-22 12:24','4 second')", "2003-10-22 12:24:04")
datetest(2.44, "datetime('2003-10-22 12:24','56 second')", "2003-10-22 12:24:56")
datetest(2.45, "datetime('2003-10-22 12:24','60 second')", "2003-10-22 12:25:00")
datetest(2.46, "datetime('2003-10-22 12:24','70 second')", "2003-10-22 12:25:10")
datetest(2.47, "datetime('2003-10-22 12:24','8.6 seconds')", "2003-10-22 12:24:08")
datetest(2.48, "datetime('2003-10-22 12:24','9.4 second')", "2003-10-22 12:24:09")
datetest(2.49, "datetime('2003-10-22 12:24','0000 second')", "2003-10-22 12:24:00")
datetest(2.50, "datetime('2003-10-22 12:24','0001 second')", "2003-10-22 12:24:01")
datetest(2.51, "datetime('2003-10-22 12:24','nonsense')", "NULL")
datetest(3.1, "strftime('%d','2003-10-31 12:34:56.432')", "31")
datetest("3.2.1", "strftime('pre%fpost','2003-10-31 12:34:56.432')", "pre56.432post")
datetest("3.2.2", "strftime('%f','2003-10-31 12:34:59.9999999')", "59.999")
datetest(3.3, "strftime('%H','2003-10-31 12:34:56.432')", "12")
datetest(3.4, "strftime('%j','2003-10-31 12:34:56.432')", "304")
datetest(3.5, "strftime('%J','2003-10-31 12:34:56.432')", "2452944.024264259")
datetest(3.6, "strftime('%m','2003-10-31 12:34:56.432')", "10")
datetest(3.7, "strftime('%M','2003-10-31 12:34:56.432')", "34")
datetest("3.8.1", "strftime('%s','2003-10-31 12:34:56.432')", "1067603696")
datetest("3.8.2", "strftime('%s','2038-01-19 03:14:07')", "2147483647")
datetest("3.8.3", "strftime('%s','2038-01-19 03:14:08')", "2147483648")
datetest("3.8.4", "strftime('%s','2201-04-09 12:00:00')", "7298164800")
datetest("3.8.5", "strftime('%s','9999-12-31 23:59:59')", "253402300799")
datetest("3.8.6", "strftime('%s','1969-12-31 23:59:59')", "-1")
datetest("3.8.7", "strftime('%s','1901-12-13 20:45:52')", "-2147483648")
datetest("3.8.8", "strftime('%s','1901-12-13 20:45:51')", "-2147483649")
datetest("3.8.9", "strftime('%s','1776-07-04 00:00:00')", "-6106060800")
datetest(3.9, "strftime('%S','2003-10-31 12:34:56.432')", "56")
datetest(3.10, "strftime('%w','2003-10-31 12:34:56.432')", "5")
datetest("3.11.1", "strftime('%W','2003-10-31 12:34:56.432')", "43")
datetest("3.11.2", "strftime('%W','2004-01-01')", "00")
datetest("3.11.3", "strftime('%W','2004-01-02')", "00")
datetest("3.11.4", "strftime('%W','2004-01-03')", "00")
datetest("3.11.5", "strftime('abc%Wxyz','2004-01-04')", "abc00xyz")
datetest("3.11.6", "strftime('%W','2004-01-05')", "01")
datetest("3.11.7", "strftime('%W','2004-01-06')", "01")
datetest("3.11.8", "strftime('%W','2004-01-07')", "01")
datetest("3.11.9", "strftime('%W','2004-01-08')", "01")
datetest("3.11.10", "strftime('%W','2004-01-09')", "01")
datetest("3.11.11", "strftime('%W','2004-07-18')", "28")
datetest("3.11.12", "strftime('%W','2004-12-31')", "52")
datetest("3.11.13", "strftime('%W','2007-12-31')", "53")
datetest("3.11.14", "strftime('%W','2007-01-01')", "01")
datetest("3.11.15", "strftime('%W %j',2454109.04140970)", "02 008")
datetest("3.11.16", "strftime('%W %j',2454109.04140971)", "02 008")
datetest("3.11.17", "strftime('%W %j',2454109.04140972)", "02 008")
datetest("3.11.18", "strftime('%W %j',2454109.04140973)", "02 008")
datetest("3.11.19", "strftime('%W %j',2454109.04140974)", "02 008")
datetest("3.11.20", "strftime('%W %j',2454109.04140975)", "02 008")
datetest("3.11.21", "strftime('%W %j',2454109.04140976)", "02 008")
datetest("3.11.22", "strftime('%W %j',2454109.04140977)", "02 008")
datetest("3.11.23", "strftime('%W %j',2454109.04140978)", "02 008")
datetest("3.11.24", "strftime('%W %j',2454109.04140979)", "02 008")
datetest("3.11.25", "strftime('%W %j',2454109.04140980)", "02 008")
datetest("3.11.99", "strftime('%W %j','2454109.04140970')", "02 008")
datetest(3.12, "strftime('%Y','2003-10-31 12:34:56.432')", "2003")
datetest(3.13, "strftime('%%','2003-10-31 12:34:56.432')", "%")
datetest(3.14, "strftime('%_','2003-10-31 12:34:56.432')", "NULL")
datetest(3.15, "strftime('%Y-%m-%d','2003-10-31')", "2003-10-31")
local function my_repeat(n, txt)
    local x = ""
    while (n > 0)
 do
        x = x .. txt
        n = n + -1
    end
    return x
end

datetest(3.16, "strftime('"..my_repeat(200, "%Y").."','2003-10-31')", my_repeat(200, 2003))
datetest(3.17, "strftime('"..my_repeat(200, "abc%m123").."','2003-10-31')", my_repeat(200, "abc10123"))
for _, c in ipairs({"a", "b", "c", "e", "g", "h", "i", "k", "l", "n", "o", "p", "q", "r", "t", "v", "x", "y", "z",
    "A", "B", "C", "D", "E", "F", "G", "I", "K", "L", "N", "O", "P", "Q", "R", "T", "U", "V", "Z",
    0, 1, 2, 3, 4, 5, 6, 6, 7, 9, "_"}) do
    datetest("3.18."..c, "strftime('%"..c.."','2003-10-31')", "NULL")
end
-- Ticket #2276.  Make sure leading zeros are inserted where appropriate.
--
datetest(3.20, "strftime('%d/%f/%H/%W/%j/%m/%M/%S/%Y','0421-01-02 03:04:05.006')", "02/05.006/03/00/002/01/04/05/0421")

datetest(5.1, "datetime('1994-04-16 14:00:00 +05:00')", "1994-04-16 09:00:00")
datetest(5.2, "datetime('1994-04-16 14:00:00 -05:15')", "1994-04-16 19:15:00")
datetest(5.3, "datetime('1994-04-16 05:00:00 +08:30')", "1994-04-15 20:30:00")
datetest(5.4, "datetime('1994-04-16 14:00:00 -11:55')", "1994-04-17 01:55:00")
datetest(5.5, "datetime('1994-04-16 14:00:00 -11:60')", "NULL")
datetest(5.6, "datetime('1994-04-16 14:00:00 -11:55  ')", "1994-04-17 01:55:00")
datetest(5.7, "datetime('1994-04-16 14:00:00 -11:55 x')", "NULL")
datetest(5.8, "datetime('1994-04-16T14:00:00Z')", "1994-04-16 14:00:00")
datetest(5.9, "datetime('1994-04-16 14:00:00z')", "1994-04-16 14:00:00")
datetest(5.10, "datetime('1994-04-16 14:00:00 Z')", "1994-04-16 14:00:00")
datetest(5.11, "datetime('1994-04-16 14:00:00z    ')", "1994-04-16 14:00:00")
datetest(5.12, "datetime('1994-04-16 14:00:00     z    ')", "1994-04-16 14:00:00")
datetest(5.13, "datetime('1994-04-16 14:00:00Zulu')", "NULL")
datetest(5.14, "datetime('1994-04-16 14:00:00Z +05:00')", "NULL")
datetest(5.15, "datetime('1994-04-16 14:00:00 +05:00 Z')", "NULL")

-- TBI to be implemented sql_current_time

-- localtime->utc and utc->localtime conversions.  These tests only work
-- if the localtime is in the US Eastern Time (the time in Charlotte, NC
-- and in New York.)
--
-- On non-Vista Windows platform, '2006-03-31' is treated incorrectly as being
-- in DST giving a 4 hour offset instead of 5.  In 2007, DST was extended to 
-- start three weeks earlier (second Sunday in March) and end one week
-- later (first Sunday in November).  Older Windows systems apply this
-- new rule incorrectly to dates prior to 2007.
--
-- It might be argued that this is masking a problem on non-Vista Windows
-- platform.  A ticket has already been opened for this issue 
-- (http://www.sql.org/cvstrac/tktview?tn=2322).  This is just to prevent
-- more confusion/reports of the issue.
--
-- $tzoffset_old should be 5 if DST is working correctly.
-- tzoffset_old = db("one", [[
--  SELECT CAST(24*(julianday('2006-03-31') -
--                  julianday('2006-03-31','localtime'))+0.5
--              AS INT)
--]])
-- $tzoffset_new should be 4 if DST is working correctly.
--tzoffset_new = db("one", [[
--  SELECT CAST(24*(julianday('2007-03-31') -
--                  julianday('2007-03-31','localtime'))+0.5
--              AS INT)
--]])
-- Warn about possibly broken Windows DST implementations.
--if (((tcl_platform(platform) == "windows") and (tzoffset_new == 4)) and (tzoffset_old == 4))
-- then
--    -- puts","******************************************************************"]]=])
--    -- puts","N.B.:  The DST support provided by your current O\/S seems to be"]]=])
--    -- puts","suspect in that it is reporting incorrect DST values for dates"]]=])
--    -- puts","prior to 2007.  This is the known case for most (all?) non-Vista"]]=])
--    -- puts","Windows versions.  Please see ticket #2322 for more information."]]=])
--    -- puts","******************************************************************"]]=])
--end
--if (tzoffset_new == 4)
-- then
--    datetest(6.1, "datetime('2000-10-29 05:59:00','localtime')", "2000-10-29 01:59:00")
--    datetest("6.1.1", "datetime('2006-10-29 05:59:00','localtime')", "2006-10-29 01:59:00")
--    datetest("6.1.2", "datetime('2007-11-04 05:59:00','localtime')", "2007-11-04 01:59:00")
--    -- If the new and old DST rules seem to be working correctly...
--    if ((tzoffset_new == 4) and (tzoffset_old == 5))
-- then
--        datetest(6.2, "datetime('2000-10-29 06:00:00','localtime')", "2000-10-29 01:00:00")
--        datetest("6.2.1", "datetime('2006-10-29 06:00:00','localtime')", "2006-10-29 01:00:00")
--    end
--    datetest("6.2.2", "datetime('2007-11-04 06:00:00','localtime')", "2007-11-04 01:00:00")
--    -- If the new and old DST rules seem to be working correctly...
--    if ((tzoffset_new == 4) and (tzoffset_old == 5))
-- then
--        datetest(6.3, "datetime('2000-04-02 06:59:00','localtime')", "2000-04-02 01:59:00")
--        datetest("6.3.1", "datetime('2006-04-02 06:59:00','localtime')", "2006-04-02 01:59:00")
--    end
--    datetest("6.3.2", "datetime('2007-03-11 07:00:00','localtime')", "2007-03-11 03:00:00")
--    datetest(6.4, "datetime('2000-04-02 07:00:00','localtime')", "2000-04-02 03:00:00")
--    datetest("6.4.1", "datetime('2006-04-02 07:00:00','localtime')", "2006-04-02 03:00:00")
--    datetest("6.4.2", "datetime('2007-03-11 07:00:00','localtime')", "2007-03-11 03:00:00")
--    datetest(6.5, "datetime('2000-10-29 01:59:00','utc')", "2000-10-29 05:59:00")
--    datetest("6.5.1", "datetime('2006-10-29 01:59:00','utc')", "2006-10-29 05:59:00")
--    datetest("6.5.2", "datetime('2007-11-04 01:59:00','utc')", "2007-11-04 05:59:00")
--    -- If the new and old DST rules seem to be working correctly...
--    if ((tzoffset_new == 4) and (tzoffset_old == 5))
-- then
--        datetest(6.6, "datetime('2000-10-29 02:00:00','utc')", "2000-10-29 07:00:00")
--        datetest("6.6.1", "datetime('2006-10-29 02:00:00','utc')", "2006-10-29 07:00:00")
--    end
--    datetest("6.6.2", "datetime('2007-11-04 02:00:00','utc')", "2007-11-04 07:00:00")
--    -- If the new and old DST rules seem to be working correctly...
--    if ((tzoffset_new == 4) and (tzoffset_old == 5))
-- then
--        datetest(6.7, "datetime('2000-04-02 01:59:00','utc')", "2000-04-02 06:59:00")
--        datetest("6.7.1", "datetime('2006-04-02 01:59:00','utc')", "2006-04-02 06:59:00")
--    end
--    datetest("6.7.2", "datetime('2007-03-11 01:59:00','utc')", "2007-03-11 06:59:00")
--    datetest(6.8, "datetime('2000-04-02 02:00:00','utc')", "2000-04-02 06:00:00")
--    datetest("6.8.1", "datetime('2006-04-02 02:00:00','utc')", "2006-04-02 06:00:00")
--    datetest("6.8.2", "datetime('2007-03-11 02:00:00','utc')", "2007-03-11 06:00:00")
--    datetest(6.10, "datetime('2000-01-01 12:00:00','localtime')", "2000-01-01 07:00:00")
--    datetest(6.11, "datetime('1969-01-01 12:00:00','localtime')", "1969-01-01 07:00:00")
--    datetest(6.12, "datetime('2039-01-01 12:00:00','localtime')", "2039-01-01 07:00:00")
--    datetest(6.13, "datetime('2000-07-01 12:00:00','localtime')", "2000-07-01 08:00:00")
--    datetest(6.14, "datetime('1969-07-01 12:00:00','localtime')", "1969-07-01 07:00:00")
--    datetest(6.15, "datetime('2039-07-01 12:00:00','localtime')", "2039-07-01 07:00:00")
--    sql_current_time = test:execsql "SELECT strftime('%s','2000-07-01 12:34:56')"
--    datetest(6.16, "datetime('now','localtime')", "2000-07-01 08:34:56")
--    datetest(6.17, "datetime('now','localtimex')", "NULL")
--    datetest(6.18, "datetime('now','localtim')", "NULL")
--    sql_current_time = 0
--end
-- These two are a bit of a scam. They are added to ensure that 100% of
-- the date.c file is covered by testing, even when the time-zone
-- is not -0400 (the condition for running of the block of tests above).
--
datetest(6.19, "datetime('2039-07-01 12:00:00','localtime',null)", "NULL")
datetest(6.20, "datetime('2039-07-01 12:00:00','utc',null)", "NULL")
-- Date-time functions that contain NULL arguments return a NULL
-- result.
--
datetest(7.1, "datetime(null)", "NULL")
datetest(7.2, "datetime('now',null)", "NULL")
datetest(7.3, "datetime('now','localtime',null)", "NULL")
datetest(7.4, "time(null)", "NULL")
datetest(7.5, "time('now',null)", "NULL")
datetest(7.6, "time('now','localtime',null)", "NULL")
datetest(7.7, "date(null)", "NULL")
datetest(7.8, "date('now',null)", "NULL")
datetest(7.9, "date('now','localtime',null)", "NULL")
datetest(7.10, "julianday(null)", "NULL")
datetest(7.11, "julianday('now',null)", "NULL")
datetest(7.12, "julianday('now','localtime',null)", "NULL")
datetest(7.13, "strftime(null,'now')", "NULL")
datetest(7.14, "strftime('%s',null)", "NULL")
datetest(7.15, "strftime('%s','now',null)", "NULL")
datetest(7.16, "strftime('%s','now','localtime',null)", "NULL")
-- Negative years work.  Example:  '-4713-11-26' is JD 1.5.
--
datetest(9.1, "julianday('-4713-11-24 12:00:00')", "0")
datetest(9.2, "julianday(datetime(5))", "5")
datetest(9.3, "julianday(datetime(10))", "10")
datetest(9.4, "julianday(datetime(100))", "100")
datetest(9.5, "julianday(datetime(1000))", "1000")
datetest(9.6, "julianday(datetime(10000))", "10000")
datetest(9.7, "julianday(datetime(100000))", "100000")
-- datetime() with just an HH:MM:SS correctly inserts the date 2000-01-01.
--
datetest(10.1, "datetime('01:02:03')", "2000-01-01 01:02:03")
datetest(10.2, "date('01:02:03')", "2000-01-01")
datetest(10.3, "strftime('%Y-%m-%d %H:%M','01:02:03')", "2000-01-01 01:02")
-- Test the new HH:MM:SS modifier
--
datetest(11.1, "datetime('2004-02-28 20:00:00', '-01:20:30')", "2004-02-28 18:39:30")
datetest(11.2, "datetime('2004-02-28 20:00:00', '+12:30:00')", "2004-02-29 08:30:00")
datetest(11.3, "datetime('2004-02-28 20:00:00', '+12:30')", "2004-02-29 08:30:00")
datetest(11.4, "datetime('2004-02-28 20:00:00', '12:30')", "2004-02-29 08:30:00")
datetest(11.5, "datetime('2004-02-28 20:00:00', '-12:00')", "2004-02-28 08:00:00")
datetest(11.6, "datetime('2004-02-28 20:00:00', '-12:01')", "2004-02-28 07:59:00")
datetest(11.7, "datetime('2004-02-28 20:00:00', '-11:59')", "2004-02-28 08:01:00")
datetest(11.8, "datetime('2004-02-28 20:00:00', '11:59')", "2004-02-29 07:59:00")
datetest(11.9, "datetime('2004-02-28 20:00:00', '12:01')", "2004-02-29 08:01:00")
datetest(11.10, "datetime('2004-02-28 20:00:00', '12:60')", "NULL")
-- Ticket #1964
datetest(12.1, "datetime('2005-09-01')", "2005-09-01 00:00:00")
datetest(12.2, "datetime('2005-09-01','+0 hours')", "2005-09-01 00:00:00")
-- Ticket #1991
test:do_execsql_test(
    "date-13.1",
    [[
        SELECT strftime('%Y-%m-%d %H:%M:%f', julianday('2006-09-24T10:50:26.047'))
    ]], {
        -- <date-13.1>
        "2006-09-24 10:50:26.047"
        -- </date-13.1>
    })

-- Ticket #2153
datetest(13.2, "strftime('%Y-%m-%d %H:%M:%S', '2007-01-01 12:34:59.6')", "2007-01-01 12:34:59")
datetest(13.3, "strftime('%Y-%m-%d %H:%M:%f', '2007-01-01 12:34:59.6')", "2007-01-01 12:34:59.600")
datetest(13.4, "strftime('%Y-%m-%d %H:%M:%S', '2007-01-01 12:59:59.6')", "2007-01-01 12:59:59")
datetest(13.5, "strftime('%Y-%m-%d %H:%M:%f', '2007-01-01 12:59:59.6')", "2007-01-01 12:59:59.600")
datetest(13.6, "strftime('%Y-%m-%d %H:%M:%S', '2007-01-01 23:59:59.6')", "2007-01-01 23:59:59")
datetest(13.7, "strftime('%Y-%m-%d %H:%M:%f', '2007-01-01 23:59:59.6')", "2007-01-01 23:59:59.600")
-- Ticket #3618
datetest(13.11, "julianday(2454832.5,'-1 day')", "2454831.5")
datetest(13.12, "julianday(2454832.5,'+1 day')", "2454833.5")
datetest(13.13, "julianday(2454832.5,'-1.5 day')", "2454831")
datetest(13.14, "julianday(2454832.5,'+1.5 day')", "2454834")
datetest(13.15, "julianday(2454832.5,'-3 hours')", "2454832.375")
datetest(13.16, "julianday(2454832.5,'+3 hours')", "2454832.625")
datetest(13.17, "julianday(2454832.5,'-45 minutes')", "2454832.46875")
datetest(13.18, "julianday(2454832.5,'+45 minutes')", "2454832.53125")
datetest(13.19, "julianday(2454832.5,'-675 seconds')", "2454832.4921875")
datetest(13.20, "julianday(2454832.5,'+675 seconds')", "2454832.5078125")
datetest(13.21, "julianday(2454832.5,'-1.5 months')", "2454786.5")
datetest(13.22, "julianday(2454832.5,'+1.5 months')", "2454878.5")
datetest(13.23, "julianday(2454832.5,'-1.5 years')", "2454284")
datetest(13.24, "julianday(2454832.5,'+1.5 years')", "2455380")
datetest(13.30, "date('2000-01-01','+1.5 years')", "2001-07-02")
datetest(13.31, "date('2001-01-01','+1.5 years')", "2002-07-02")
datetest(13.32, "date('2002-01-01','+1.5 years')", "2003-07-02")
datetest(13.33, "date('2002-01-01','-1.5 years')", "2000-07-02")
datetest(13.34, "date('2001-01-01','-1.5 years')", "1999-07-02")
-- # Test for issues reported by BareFeet (list.sql at tandb.com.au)
-- # on mailing list on 2008-06-12.
-- #
-- # Put a floating point number in the database so that we can manipulate
-- # raw bits using the hexio interface.
-- #
-- if {0==[sql -has-codec]} {
--   do_test date-14.1 {
--     execsql {
--       CREATE TABLE t1(x FLOAT );
--       INSERT INTO t1 VALUES(1.1);
--     }
--     db close
--     hexio_write test.db 2040 4142ba32bffffff9
--     sql db test.db
--     db eval {SELECT * FROM t1}
--   } {2454629.5}
--   # Changing the least significant byte of the floating point value between
--   # 00 and FF should always generate a time of either 23:59:59 or 00:00:00,
--   # never 24:00:00
--   #
--   for {set i 0} {$i<=255} {incr i} {
--     db close
--     hexio_write test.db 2047 [format %02x $i]
--     sql db test.db
--     do_test date-14.2.$i {
--       set date [db one {SELECT datetime(x) FROM t1}]
--       expr {$date eq "2008-06-12 00:00:00" || $date eq "2008-06-11 23:59:59"}
--     } {1}
--   }
-- }
-- Verify that multiple calls to date functions with 'now' return the
-- same answer.
--
-- EVIDENCE-OF: R-34818-13664 The 'now' argument to date and time
-- functions always returns exactly the same value for multiple
-- invocations within the same sql_step() call.
--
local function sleeper()
    -- after 100 ms
    os.execute("sleep 0.1")
end
box.internal.sql_create_function("sleeper", "INT", sleeper)
-- db("func", "sleeper", "sleeper")
test:do_test(
    "date-15.1",
    function()
        return test:execsql [[
            SELECT c - a FROM (SELECT julianday('now') AS a,
                                      sleeper(), julianday('now') AS c);
        ]]
    end, {
        -- <date-15.1>
        0.0
        -- </date-15.1>
    })

test:do_test(
    "date-15.2",
    function()
        return test:execsql [[
            SELECT a==b FROM (SELECT current_timestamp AS a,
                                      sleeper(), current_timestamp AS b);
        ]]
    end, {
        -- <date-15.2>
        1
        -- </date-15.2>
    })
end -- if false


test:finish_test()
