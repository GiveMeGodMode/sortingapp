const fs = require("fs");
const readline = require("readline");
const path = require("path");
const inputFilePath = path.join(__dirname, "input.txt");
const outputFilePath = path.join(__dirname, "output.txt");
const tempFolderPath = path.join(__dirname, "./temp/");

const chunkSize = 500 * 1024 * 1024; // 500 МБ

async function sortLargeFile() {
  const start = process.hrtime();
  // Создаем временную папку, если она не существует
  if (!fs.existsSync(tempFolderPath)) {
    fs.mkdirSync(tempFolderPath);
  }

  let chunkIndex = 0;
  let chunkData = [];

  // Читаем исходный файл построчно
  const readStream = fs.createReadStream(inputFilePath);
  const rl = readline.createInterface({
    input: readStream,
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    chunkData.push(line);

    // Если размер данных достигает размера части (chunk), сортируем и записываем во временный файл
    if (Buffer.byteLength(chunkData.join("\n")) >= chunkSize) {
      chunkData.sort();

      const tempFilePath = path.join(`temp_${chunkIndex}.txt`);
      fs.writeFileSync(tempFilePath, chunkData.join("\n"));

      chunkData = [];
      chunkIndex++;
    }
  }

  // Сортируем и записываем оставшиеся данные в последний временный файл
  if (chunkData.length > 0) {
    chunkData.sort();

    const tempFilePath = `${tempFolderPath}temp_${chunkIndex}.txt`;
    fs.writeFileSync(tempFilePath, chunkData.join("\n"));
  }

  // Объединяем временные файлы в один отсортированный файл
  const outputStream = fs.createWriteStream(outputFilePath);
  const tempFiles = fs
    .readdirSync(tempFolderPath)
    .map((tempFile) => `${tempFolderPath}${tempFile}`);

  mergeSortedFiles(tempFiles, outputStream);
  // Закрываем чтение исходного файла
  readStream.close();
  // Закрываем чтение выходного файла
  outputStream.close();

  /*здесь костыль, на случай не создания папки temp, и не обновления в output.txt*/

  // Объявляем функцию для удаления временных файлов и папки
  async function deleteTempFiles() {
    try {
      // Удаляем каждый временный файл
      for (const tempFile of tempFiles) {
        await fs.promises.unlink(tempFile);
      }

      // Удаляем временную папку
      await fs.promises.rmdir(tempFolderPath);

      console.log("Удаление временных файлов и папки завершено.");
    } catch (error) {
      console.error("Ошибка при удалении временных файлов и папки:", error);
    }
  }
  deleteTempFiles();
  //Вывод служебных сообщений о сортировке
  //Время
  const end = process.hrtime(start);
  const executionTime = end[0] + end[1] / 1e9; // Время выполнения в секундах
  const used = process.memoryUsage();
  //Память
  console.log("Объем используемой памяти:");
  for (let key in used) {
    console.log(
      `${key}: ${Math.round((used[key] / 1024 / 1024) * 100) / 100} MB`
    );
  }

  console.log(`Время выполнения программы: ${executionTime} сек.`);
  console.log("Сортировка завершена.");
}

function mergeSortedFiles(inputFiles, outputStream) {
  const fileStreams = inputFiles.map((file) => fs.createReadStream(file));
  const lineReaders = fileStreams.map((fileStream) =>
    readline.createInterface({ input: fileStream })
  );

  const heap = new BinaryHeap();

  // Инициализируем структуру с начальными значениями из первых строк файлов
  for (let i = 0; i < lineReaders.length; i++) {
    const lineReader = lineReaders[i];
    const firstLine = lineReader[Symbol.asyncIterator]()
      .next()
      .then(({ value, done }) => {
        if (!done) {
          heap.push({ value, lineReaderIndex: i });
        }
      });
  }

  // Пока структура не пуста, извлекаем минимальный элемент и записываем его в выходной файл
  while (!heap.isEmpty()) {
    const { value, lineReaderIndex } = heap.pop();

    outputStream.write(`${value}\n`);

    const lineReader = lineReaders[lineReaderIndex];
    const nextLine = lineReader[Symbol.asyncIterator]()
      .next()
      .then(({ value, done }) => {
        if (!done) {
          heap.push({ value, lineReaderIndex });
        } else {
          lineReader.close();
          fileStreams[lineReaderIndex].close();
        }
      });
  }

  for (let i = 0; i < inputFiles.length; i++) {
    const tempFileData = fs.readFileSync(inputFiles[i], "utf8");
    outputStream.write(tempFileData);
  }

  outputStream.end();
}

// Структура для сортировки
class BinaryHeap {
  constructor() {
    this.heap = [];
  }

  push(value) {
    this.heap.push(value);
    this.bubbleUp(this.heap.length - 1);
  }

  pop() {
    const min = this.heap[0];
    const last = this.heap.pop();

    if (this.heap.length > 0) {
      this.heap[0] = last;
      this.bubbleDown(0);
    }

    return min;
  }

  isEmpty() {
    return this.heap.length === 0;
  }

  bubbleUp(index) {
    const element = this.heap[index];

    while (index > 0) {
      const parentIndex = Math.floor((index - 1) / 2);
      const parent = this.heap[parentIndex];

      if (element.value >= parent.value) {
        break;
      }

      this.heap[index] = parent;
      index = parentIndex;
    }

    this.heap[index] = element;
  }

  bubbleDown(index) {
    const element = this.heap[index];
    const lastIndex = this.heap.length - 1;

    while (true) {
      const childIndex = index * 2 + 1;

      if (childIndex > lastIndex) {
        break;
      }

      const child = this.heap[childIndex];
      const rightChildIndex = childIndex + 1;

      if (
        rightChildIndex <= lastIndex &&
        this.heap[rightChildIndex].value < child.value
      ) {
        this.heap[childIndex] = this.heap[rightChildIndex];
        this.heap[rightChildIndex] = child;
      }

      if (element.value <= child.value) {
        break;
      }

      this.heap[index] = child;
      index = childIndex;
    }

    this.heap[index] = element;
  }
}

sortLargeFile();
