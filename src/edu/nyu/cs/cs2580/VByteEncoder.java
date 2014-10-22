package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;

import org.apache.commons.io.IOUtils;

public class VByteEncoder
{

    public VByteEncoder()
    {
    }
    
    //public static ArrayList<Byte> encode(ArrayList<Integer> nums)
    public static byte[] encode(ArrayList<Integer> nums)
    {
        //ArrayList<Byte> byteStream = new ArrayList<Byte>();
        int totalSize = 0;
        for(int i=0;i<nums.size();i++)
        {
            int n = nums.get(i);
            int sizeReq = 1;
            if(n < Math.pow(2, 7))
                sizeReq = 1;
            else if(n < Math.pow(2, 14))
                sizeReq = 2;
            else if(n < Math.pow(2, 21))
                sizeReq = 3;
            else if(n < Math.pow(2, 28))
                sizeReq = 4;
            totalSize += sizeReq;
        }
        byte totalArray[] = new byte[totalSize];
        int c = 0;
        for(int i=0;i<nums.size();i++)
        {
            byte array[] = encode(nums.get(i));
            //byteStream.addAll(array);
            for(int j=0;j<array.length;j++)
            totalArray[c++] = array[j];
        }
        return totalArray;
    }
    
    public static byte[] encode(int n)
    {
        int sizeReq = 1;
        if(n < Math.pow(2, 7))
            sizeReq = 1;
        else if(n < Math.pow(2, 14))
            sizeReq = 2;
        else if(n < Math.pow(2, 21))
            sizeReq = 3;
        else if(n < Math.pow(2, 28))
            sizeReq = 4;
            
        //ArrayList<Byte> array = new ArrayList<Byte>();
        byte array[] = new byte[sizeReq];
        int i = sizeReq-1;
        while(true)
        {
            int x = n % 128;
            array[i] = (byte)x;
            if(n < 128)
                break;
            
            n = n / 128;
            i--;
        }
        byte newLast = (byte) ((byte)array[sizeReq-1] | (byte)(1 << 7));
        array[sizeReq-1] = (byte)newLast;
        
        return array;
        
    }
    
    public static int encode(int n, byte[] array1)
    {
        int sizeReq = 1;
        if(n < Math.pow(2, 7))
            sizeReq = 1;
        else if(n < Math.pow(2, 14))
            sizeReq = 2;
        else if(n < Math.pow(2, 21))
            sizeReq = 3;
        else if(n < Math.pow(2, 28))
            sizeReq = 4;
            
        //ArrayList<Byte> array = new ArrayList<Byte>();
        //byte array[] = new byte[sizeReq];
        int i = sizeReq-1;
        while(true)
        {
            int x = n % 128;
            array1[i] = (byte)x;
            if(n < 128)
                break;
            
            n = n / 128;
            i--;
        }
        byte newLast = (byte) ((byte)array1[sizeReq-1] | (byte)(1 << 7));
        array1[sizeReq-1] = (byte)newLast;
        
        return sizeReq;
        
    }
    
    public static ArrayList<Integer> decode(ArrayList<Byte> list)
    {
        byte array[] = new byte[list.size()];
        for(int i=0;i<list.size();i++)
            array[i] = list.get(i);
        
        return decode(array);
    }
    public static ArrayList<Integer> decode(byte array[])
    {
        int n = 0;
        ArrayList<Integer> intArray = new ArrayList<Integer>();
        for(int i=0;i<array.length;i++)
        {
            byte b = array[i];
            boolean last = ((b & (byte)(1 << 7)) != 0);
            if(!last)
                n = 128 * n + (int)b;
            else
            {
                n = 128 * n + (int) (b & (byte)127);
                intArray.add(n);
                //System.out.println(n);
                n = 0;
            }
        }
        return intArray;
    }
    
    public static int[] getFirstNum(byte array[], int start)
    {
        int lastByteOfFirstNum = 0;
        for(int i=start;i<array.length;i++)
        {
            byte b = array[i];
            boolean last = ((b & (byte)(1 << 7)) != 0);
            if(last)
            {
                lastByteOfFirstNum = i;
                break;
            }
            
        } 
        byte firstNum[] = new byte[lastByteOfFirstNum - start + 1];
        for(int i=start;i<=lastByteOfFirstNum;i++)
        {
            firstNum[i-start] = array[i];
        }
        //System.out.println("lastByteOfFirstNum = " + lastByteOfFirstNum);
        int nextLoc = lastByteOfFirstNum + 1;
        int answer = decode(firstNum).get(0);
        
        int x[] = new int[2];
        x[0] = (answer);
        x[1] = (nextLoc);
        return x;
        
    }
    
    public static void main(String[] args)
    {

        //convert(300);
        ArrayList<Integer> nums = new ArrayList<Integer>();
        nums.add(1);
        nums.add(2);
        nums.add(10000);
        nums.add(200);
        
        ArrayList<Integer> nums2 = new ArrayList<Integer>();
        nums2.add(3);
        nums2.add(4);
        nums2.add(5);
        
        
        
        byte array[] = VByteEncoder.encode(nums);
        byte array2[] = VByteEncoder.encode(nums2);
        
        System.out.println("Actual:");
        for(int i=0;i<array.length;i++)
        {
            System.out.println(Integer.toHexString(array[i]));
        }
        
        System.out.println("Actual2:");
        for(int i=0;i<array2.length;i++)
        {
            System.out.println(Integer.toHexString(array2[i]));
        }
        
        
        int lenToRead = array.length;
        try
        {
            FileOutputStream output = new FileOutputStream(new File("testByteOut"));
            IOUtils.write(array, output);
            System.out.println("Wrote " + array.length + " bytes");
            IOUtils.write(array2, output);
            System.out.println("Wrote " + array2.length + " bytes");
        }
        catch(Exception e)
        {
        }
        
        try
        {
            byte newArray[];// = new byte[lenToRead];
            FileInputStream input = new FileInputStream(new File("testByteOut"));
            newArray = IOUtils.toByteArray(input, lenToRead);
            
            System.out.println("Read:");
            for(int i=0;i<newArray.length;i++)
            {
                System.out.println(Integer.toHexString(newArray[i]));
            }
            System.out.println("");
            
            newArray = IOUtils.toByteArray(input);
            System.out.println("Read again:");
            for(int i=0;i<newArray.length;i++)
            {
                System.out.println(Integer.toHexString(newArray[i]));
            }
            System.out.println("");
            
        }
        
        catch(Exception e)
        {
        }
        

        /*
        for(int i=0;i<array.length;i++)
        {
            System.out.println(Integer.toHexString(array[i]));
        }

            
        ArrayList<Integer> nums2 = decode(array);
        for(int i=0;i<nums2.size();i++)
        {
            System.out.println(nums2.get(i));
        }
        for(int i=0;i<array.length;i++)
        {
            byte b1 = array[i];
            String s1 = String.format("%8s", Integer.toBinaryString(b1 & 0xFF)).replace(' ', '0');
            System.out.println(s1); // 10000001
            if((b1 & (byte)(1 << 7)) != 0)
                System.out.println("last");
        }
      */  
    }
    
    
    
}
