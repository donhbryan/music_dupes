DROP VIEW "main"."album_tracks_V";
CREATE VIEW album_tracks_V as
SELECT release_id, album_artist , album_title, substr('00'||track_no,-2,2)||' '||title as title, path 
from albums as a 
INNER JOIN files as f
on a.release_id = f.album_id
order by 1 , 2 , 3
commit;
