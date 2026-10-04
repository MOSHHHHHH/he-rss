# he-rss# Hebrew news RSS finder

מאתר פידי RSS של אתרי חדשות בעברית.

1. Google News RSS (עברית, `hl=iw&gl=IL&ceid=IL:he`) - חיפוש במאות שילובי נושא x טווח זמן (`config/queries.txt`)
2. מיפוי האתרים לפי תג `<source url>` בכל כתבה
3. איתור פיד לכל אתר, ברצף: נתיבים נפוצים -> קוד דף הבית -> Serper (3 תוצאות ראשונות) -> ויתור

## הפעלה
1. הוסיפו סיקרט `SERPER_API_KEY` ב-Settings -> Secrets and variables -> Actions
2. Actions -> "Build Hebrew news RSS list" -> Run workflow (מומלץ קודם עם `max_sites=10` לבדיקה)

## פלט (בתיקיית `data/`, נשמר אוטומטית בקומיט)
- `feeds.json` / `feeds.csv` / `feeds.opml` - הפידים שנמצאו
- `sites.json` - כל האתרים שמופו

ריצות חוזרות מדלגות על אתרים שכבר נבדקו (אלא אם סימנתם retry / force).
להוספת נושאים - ערכו את `config/queries.txt`.
