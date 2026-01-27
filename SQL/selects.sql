
select path, title, track_no, format from files
--  where acoustid_id is not null
where lower(title) like lower('%A psalm for bob marley%')
-- where score is NULL

 order by path, album_id,track_no, title;
 COMMIT;
 select * from albums where lower(album_title) like '%road%' order by 2; commit;
 
 select release_id, album_title from album_tracks_V where title like  '%I Love King Selassie%';
 
 select * from album_tracks_V where lower(album_artist) like lower('%black uhuru%')
 order by album_title,path