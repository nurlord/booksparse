const mongoose = require("mongoose");
const axios = require("axios");

// Define Schemas
const genreSchema = new mongoose.Schema({
  id: Number,
  name: String,
  slug: String,
  niche: {
    id: Number,
    name: String,
  },
});

const tagSchema = new mongoose.Schema({
  id: Number,
  name: String,
  slug: String,
});

const bookSchema = new mongoose.Schema({
  id: { type: Number, unique: true },
  name: String,
  annotation: String,
  available: Boolean,
  genres: [{ type: mongoose.Schema.Types.ObjectId, ref: "Genre" }],
  tags: [{ type: mongoose.Schema.Types.ObjectId, ref: "Tag" }],
});

const authorSchema = new mongoose.Schema({
  id: { type: Number, unique: true },
  name: String,
  slug: String,
  book_count: Number,
  books: [{ type: mongoose.Schema.Types.ObjectId, ref: "Book" }],
});

// Define Models
const Genre = mongoose.model("Genre", genreSchema);
const Tag = mongoose.model("Tag", tagSchema);
const Book = mongoose.model("Book", bookSchema);
const Author = mongoose.model("Author", authorSchema);

// Retry logic with exponential backoff
const retryWithBackoff = async (fn, retries = 5, delay = 200) => {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      if (attempt === retries - 1) throw error;
      console.error(`Error: ${error.message}. Retrying in ${delay}ms...`);
      await new Promise((resolve) => setTimeout(resolve, delay));
      delay *= 2; // Exponential backoff
    }
  }
};

// Fetch genres
const fetchAndSaveGenres = async () => {
  let genreUrl = "https://mybook.ru/api/v1/catalog/genres/?limit=100";
  while (genreUrl) {
    try {
      const res = await retryWithBackoff(() => axios.get(genreUrl));
      const genres = res.data.objects;

      await Genre.insertMany(
        genres.map((genre) => ({
          id: genre.id,
          name: genre.name,
          slug: genre.slug,
          niche: genre.niche
            ? { id: genre.niche.id, name: genre.niche.name }
            : null,
        })),
        { ordered: false }, // Ignore duplicate key errors
      );

      console.log(`Saved ${genres.length} genres.`);
      genreUrl = res.data.meta.next
        ? "https://mybook.ru" + res.data.meta.next
        : null;
    } catch (error) {
      console.error("Error fetching genres:", error.message);
      genreUrl = null; // Skip further requests for genres
    }
  }
};

// Fetch tags
const fetchAndSaveTags = async () => {
  let tagUrl = "https://mybook.ru/api/v1/tags/?limit=100";
  while (tagUrl) {
    try {
      const res = await retryWithBackoff(() => axios.get(tagUrl));
      const tags = res.data.objects;

      await Tag.insertMany(
        tags.map((tag) => ({
          id: tag.id,
          name: tag.name,
          slug: tag.slug,
        })),
        { ordered: false }, // Ignore duplicate key errors
      );

      console.log(`Saved ${tags.length} tags.`);
      tagUrl = res.data.meta.next
        ? "https://mybook.ru" + res.data.meta.next
        : null;
    } catch (error) {
      console.error("Error fetching tags:", error.message);
      tagUrl = null; // Skip further requests for tags
    }
  }
};

// Fetch books and authors
const fetchAndSaveBooksAndAuthors = async () => {
  let bookUrl = "https://mybook.ru/api/v1/books/?limit=100";
  let processedBooks = 0;

  while (bookUrl) {
    try {
      const res = await retryWithBackoff(() => axios.get(bookUrl));
      const books = res.data.objects;

      // Check if the fetched data is empty
      if (!books || books.length === 0) {
        console.log("No more books to fetch.");
        break; // Exit the loop if no books are retrieved
      }

      const authorMap = {}; // Map to group books by author

      books.forEach((book) => {
        const authorId = book.author.id;
        if (!authorMap[authorId]) {
          authorMap[authorId] = {
            id: book.author.id,
            name: book.author.cover_name,
            slug: book.author.slug,
            books: [],
          };
        }
        authorMap[authorId].books.push({
          id: book.id,
          name: book.name,
          annotation: book.annotation,
          available: book.available,
          genres: book.genres.map((genre) => mongoose.Types.ObjectId(genre.id)),
          tags: book.tags.map((tag) => mongoose.Types.ObjectId(tag.id)),
        });
      });

      // Save authors and their books
      await Promise.all(
        Object.values(authorMap).map(async (authorData) => {
          await Author.updateOne(
            { id: authorData.id }, // Find by unique identifier
            {
              id: authorData.id,
              name: authorData.name,
              slug: authorData.slug,
              book_count: authorData.books.length,
              books: authorData.books,
            },
            { upsert: true }, // Insert if not found
          );
        }),
      );

      processedBooks += books.length;
      console.log(`Saved ${books.length} books and their authors.`);

      // Get the next URL
      bookUrl = res.data.meta.next
        ? "https://mybook.ru" + res.data.meta.next
        : null;
    } catch (error) {
      console.error("Error fetching books or authors:", error.message);
      bookUrl = null; // Stop further requests on error
    }
  }

  console.log(`Total books processed: ${processedBooks}`);
};

// Main Function
const fetchData = async () => {
  await mongoose.connect("mongodb://localhost:27017/mybookdb2");

  try {
    await fetchAndSaveGenres();
    await fetchAndSaveTags();
    await fetchAndSaveBooksAndAuthors();
  } catch (error) {
    console.error("Error during data fetching:", error.message);
  } finally {
    await mongoose.disconnect();
    console.log("Data fetching and saving complete!");
  }
};

// Execute
fetchData();
