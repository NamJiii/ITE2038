SELECT T.name FROM Trainer AS T WHERE 3 <= ( SELECT COUNT(*) AS N FROM CatchedPokemon AS C WHERE T.id = C.owner_id ORDER BY N);

SELECT name FROM Pokemon WHERE type in( SELECT * FROM(SELECT type FROM Pokemon GROUP BY type ORDER BY COUNT(*) DESC LIMIT 2 )AS satisfy) ORDER BY name;

SELECT name FROM Pokemon WHERE name LIKE '_o%' ORDER BY name;

SELECT nickname FROM CatchedPokemon WHERE level>=50 ORDER BY nickname;

SELECT name FROM Pokemon WHERE LENGTH(name)=6 ORDER BY name;

SELECT name from Trainer Where hometown = 'Blue City' ORDER BY name;

SELECT DISTINCT hometown FROM Trainer ORDER BY hometown;

SELECT AVG(level) FROM CatchedPokemon WHERE owner_id=(SELECT id FROM Trainer WHERE name = 'Red');

SELECT nickname FROM CatchedPokemon WHERE nickname NOT IN('T%') ORDER BY nickname;

SELECT nickname FROM CatchedPokemon WHERE level>=50 AND owner_id>=6;

SELECT id,name FROM Pokemon ORDER BY id ASC;

SELECT nickname FROM CatchedPokemon WHERE level <=50 ORDER BY level ASC;

SELECT DISTINCT P.name,P.id FROM CatchedPokemon AS C, Pokemon AS P WHERE P.id=C.pid AND C.owner_id in( SELECT id FROM Trainer WHERE hometown = 'Sangnok City') ORDER BY P.id;

SELECT C.nickname FROM Gym AS G, Trainer AS T, CatchedPokemon AS C WHERE T.id=G.leader_id AND T.id=C.owner_id AND C.pid IN ( SELECT id FROM Pokemon WHERE type='water') ORDER BY C.nickname ASC;

SELECT COUNT(*) AS number FROM CatchedPokemon WHERE pid in( SELECT P.id FROM Pokemon AS P, Evolution AS E WHERE P.id=E.before_id);

SELECT COUNT(*) AS SUM FROM Pokemon WHERE type='water' or type='electric' or type='psychic';

SELECT COUNT(*) AS number FROM(SELECT DISTINCT C.pid FROM CatchedPokemon AS C, Trainer AS T WHERE T.hometown='Sangnok City' AND T.id=C.owner_id) AS satisfy;

SELECT C.level FROM CatchedPokemon AS C, Trainer AS T WHERE C.owner_id=T.id AND T.hometown='Sangnok City' ORDER BY level DESC LIMIT 1;

SELECT COUNT(*) AS num FROM(SELECT DISTINCT P.type FROM CatchedPokemon C, Pokemon P WHERE P.id=C.pid AND C.owner_id IN( SELECT leader_id FROM Gym WHERE city = 'Sangnok City')) AS satisfy;

SELECT T.name,COUNT(*) AS num FROM Trainer AS T, CatchedPokemon AS C WHERE T.hometown='Sangnok City' AND T.id=C.owner_id GROUP BY T.id ORDER BY num ASC;

SELECT name FROM Pokemon WHERE name LIKE 'a%' OR name LIKE 'e%' OR name LIKE 'i%' OR name LIKE 'o%' OR name LIKE 'u%';

SELECT type, COUNT(*) AS num FROM Pokemon GROUP BY type ORDER BY num ASC,type ASC;

SELECT DISTINCT T.name FROM Trainer AS T, CatchedPokemon AS C WHERE T.id=C.owner_id AND C.level<=10 ORDER BY T.name;

SELECT T.hometown, AVG(C.level) as AvgLv FROM Trainer AS T,CatchedPokemon AS C WHERE T.id = C.owner_id GROUP BY T.hometown ORDER BY AvgLv ASC;

SELECT DISTINCT name FROM Pokemon WHERE id IN( SELECT C.pid FROM CatchedPokemon AS C, Trainer AS T WHERE C.owner_id=T.id AND T.hometown = 'Sangnok City') AND id IN(SELECT C.pid FROM CatchedPokemon AS C, Trainer AS T WHERE C.owner_id=T.id AND T.hometown = 'Brown City');

SELECT nickname FROM CatchedPokemon WHERE nickname LIKE "% %" ORDER BY nickname DESC;

SELECT T.name, MAX(C.level) FROM Trainer AS T,CatchedPokemon AS C WHERE 4<= (SELECT COUNT(*) FROM CatchedPokemon WHERE T.id=owner_id) AND C.owner_id = T.id GROUP BY T.name ORDER BY T.name;

SELECT T.name, AVG(C.level) FROM Trainer AS T, CatchedPokemon AS C WHERE T.id = C.owner_id AND C.pid IN(SELECT id FROM Pokemon WHERE type='normal' OR type = 'electric') GROUP BY T.name ORDER BY AVG(C.level) ASC;

SELECT P.name, T.name, CT.description FROM Pokemon AS P,CatchedPokemon AS C, Trainer AS T, City AS CT WHERE P.id = C.pid AND C.pid=152 AND C.owner_id = T.id AND T.hometown = CT.name ORDER BY C.level DESC;

SELECT p1.id,p1.name,p2.name,p3.name FROM Pokemon AS p1, Pokemon AS p2, Pokemon As p3, Evolution AS E1, Evolution AS E2 WHERE E1.before_id = p1.id AND E1.after_id =p2.id AND E2.before_id=p2.id AND E2.after_id=p3.id ORDER BY p1.id ASC;

SELECT name FROM Pokemon WHERE id>9 AND id<101 ORDER BY name;

SELECT name FROM Pokemon WHERE id NOT IN(SELECT pid FROM CatchedPokemon) ORDER BY name;

SELECT SUM(C.level) FROM Trainer AS T, CatchedPokemon AS C WHERE C.owner_id=T.id AND T.name='Matis' GROUP BY T.name;

SELECT name FROM Trainer WHERE id IN(SELECT leader_id FROM Gym) ORDER BY name;

SELECT CONCAT((SELECT COUNT(*) FROM Pokemon GROUP BY type DESC LIMIT 1)/(SELECT COUNT(*) FROM Pokemon)*100," %") AS percentage;

SELECT name FROM Trainer Where id IN(SELECT owner_id FROM CatchedPokemon WHERE nickname LIKE "A%") ORDER BY name;

SELECT T.name,SUM(C.level) FROM Trainer AS T,CatchedPokemon AS C WHERE T.id = C.owner_id GROUP BY T.name ORDER BY SUM(C.level) DESC LIMIT 1;

SELECT name FROM Pokemon WHERE id IN(SELECT after_id FROM Evolution Where before_id NOT IN(SELECT after_id FROM Evolution)) ORDER BY name;

SELECT name FROM Trainer WHERE id IN(SELECT C1.owner_id FROM CatchedPokemon AS C1, CatchedPokemon AS C2 WHERE C1.owner_id=C2.owner_id AND C1.pid=C2.pid AND C1.nickname!=C2.nickname) ORDER BY name;

SELECT T.hometown,C.nickname From Trainer AS T, CatchedPokemon AS C WHERE (T.hometown,C.level) IN ( SELECT T.hometown,MAX(C.level) FROM Trainer AS T,CatchedPokemon AS C WHERE T.id = C.owner_id GROUP BY T.hometown) GROUP BY T.hometown ORDER BY T.hometown;

