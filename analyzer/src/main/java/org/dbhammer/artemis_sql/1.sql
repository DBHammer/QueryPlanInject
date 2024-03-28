SELECT u.UserName, p.ProductName, o.OrderDate, od.Quantity, pr.ReviewText, pr.Rating
FROM Users u
         JOIN Orders o ON u.UserID = o.UserID
         JOIN OrderDetails od ON o.OrderID = od.OrderID
         JOIN Products p ON od.ProductID = p.ProductID
         LEFT JOIN ProductReviews pr ON p.ProductID = pr.ProductID AND u.UserID = pr.UserID
ORDER BY o.OrderDate DESC, p.ProductName;