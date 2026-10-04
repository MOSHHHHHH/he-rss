[README.md](https://github.com/user-attachments/files/33021492/README.md)
# Hebrew news RSS finder

מאתר פידי RSS של אתרי חדשות בעברית.

1. Google News RSS (עברית, `hl=iw&gl=IL&ceid=IL:he`) - חיפוש במאות שילובי נושא x טווח זמן (`config/queries.txt`)
2. מיפוי האתרים לפי תג `<source url>` בכל כתבה
3. איתור פיד לכל אתר, ברצף: נתיבים נפוצים -> קוד דף הבית -> Serper (3 תוצאות ראשונות) -> ויתור

## הפעלה
1. הוסיפו סיקרט `SERPER_API_KEY` ב-Settings -> Secrets and variables -> Actions
2. Actions -> "Build Hebrew news RSS list" -> Run workflow (מומלץ קודם עם `max_sites=10` לבדיקה)

## פלט (בתיקיית `data/`, נשמר אוטומטית בקומיט)
- `feeds.json` / `feeds.csv` / `feeds.opml` - פיד ראשי לכל אתר (עם שם האתר)
- `feeds_all.csv` - כל הפידים, כולל פידים נוספים שנמצאו בדפי אינדקס RSS
- `sites.json` - כל האתרים שמופו

ריצות חוזרות מדלגות על אתרים שכבר נבדקו (אלא אם סימנתם retry / force).
להוספת נושאים - ערכו את `config/queries.txt`.

## ריצה חוזרת
סמנו `skip_discovery` כדי לא לחפש שוב ב-Google News, ו-`retry_not_found` כדי לנסות שוב אתרים שנכשלו. אתרים שנבדקו עם לוגיקה ישנה נבדקים מחדש אוטומטית.
שלב 3 רץ במקביל (`workers`), אבל בתוך אתר אחד ברצף. שלב 1 (Google News) ברצף בכוונה.
פיד עם `verified: false` נלקח מ-Serper באתר שחוסם גישה אוטומטית, ולא אומת.
