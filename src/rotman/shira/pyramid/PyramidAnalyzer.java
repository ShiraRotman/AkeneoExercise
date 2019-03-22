package rotman.shira.pyramid;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import java.util.*;
import java.util.concurrent.Phaser;
import java.util.concurrent.RecursiveTask;
import java.util.stream.Collectors;

class PyramidPathTask extends RecursiveTask<List<Long>>
{
    private Phaser synchronizer;
    private Map<Integer,PyramidPathTask> cache;
    private int line,cell,number;

    public PyramidPathTask(Phaser synchronizer,Map<Integer,PyramidPathTask> cache,
           int line,int cell,int number)
    {
        this.synchronizer=synchronizer; this.cache=cache;
        this.line=line; this.cell=cell; this.number=number;
    }

    @Override protected List<Long> compute()
    {
        List<Long> results=new ArrayList<>();
        int nextLineCell=calcAbsoluteCell(line+1,cell);
        computeSubPaths(nextLineCell,results);
        if (results.isEmpty()) results.add(new Long(number));
        else
        {
            nextLineCell++;
            computeSubPaths(nextLineCell,results);
            for (int index=0;index<results.size();index++)
                results.set(index,results.get(index)+number);
        }
        return results;
    }

    private void computeSubPaths(int startCell,List<Long> results)
    {
        int phase=synchronizer.getPhase();
        while ((!synchronizer.isTerminated())&&(phase<=startCell))
            phase=synchronizer.awaitAdvance(phase);
        PyramidPathTask pathTask=cache.get(startCell);
        if (pathTask!=null) results.addAll(pathTask.join());
    }

    public static int calcAbsoluteCell(int line,int cell)
    { return line*(line+1)/2+cell; }
}

public class PyramidAnalyzer
{
    private static final int BUFFER_SIZE=16;

    public static void main(String[] args)
    {
        if (args.length==0)
        {
            System.out.println("You must provide a file to analyze!");
            System.exit(-1);
        }
        Path filepath=FileSystems.getDefault().getPath(args[0]);
        Scanner analyzer=null;
        try { analyzer=new Scanner(filepath); }
        catch (IOException ioe)
        {
            System.out.println("Could not open the provided file!");
            System.out.println(ioe.getMessage());
            System.exit(-1);
        }
        analyzer.useDelimiter("\\s+");

        Map<Integer,PyramidPathTask> pathTaskCache=new HashMap<>();
        Phaser synchronizer=new Phaser(1);
        PyramidPathTask mainTask=null;
        int line=0,cell=0; boolean finished=false;
        do
        {
            int number=0;
            try { number=analyzer.nextInt(); }
            catch (InputMismatchException ime)
            { System.out.format("Found invalid number at (%d,%d), regarded as 0.\n",line+1,cell+1); }
            catch (NoSuchElementException nsee)
            {
                System.out.println("Unexpected end of file, returning partial results.");
                finished=true;
            }
            if (!finished)
            {
                PyramidPathTask pathTask=new PyramidPathTask(synchronizer,pathTaskCache,line,cell,number);
                if (line==0) mainTask=pathTask;
                pathTask.fork();
                int absoluteCell=PyramidPathTask.calcAbsoluteCell(line,cell);
                pathTaskCache.put(absoluteCell,pathTask);
                synchronizer.arrive();
                cell++;
                if (cell==line+1)
                {
                    String restOfLine=null;
                    try { restOfLine=analyzer.nextLine(); }
                    catch (NoSuchElementException nsee) { finished=true; }
                    if ((restOfLine!=null)&&(!restOfLine.equals("")))
                        System.out.format("Redundant data at end of line %d, skipping to next line.\n",line+1);
                    line++; cell=0;
                }
            }
        } while (!finished);

        analyzer.close();
        synchronizer.arriveAndDeregister();
        List<Long> pathSums=mainTask.join();
        Map<Long,Long> countedBySum=pathSums.parallelStream().collect(Collectors.
                groupingBy(Long::longValue,Collectors.counting()));
        System.out.println("Sum | Count");
        countedBySum.entrySet().stream().sorted(Map.Entry.<Long,Long> comparingByValue().reversed()).
                forEach(entry->System.out.println(entry.getKey() + " | " + entry.getValue()));
    }
}
